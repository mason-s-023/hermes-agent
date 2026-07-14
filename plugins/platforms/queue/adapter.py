"""Queue platform adapter — slack_agent 로컬 SQLite 큐 <-> Hermes 게이트웨이 브리지.

slack_agent 레포(bridge.local_repo.SQLiteQueueRepo)의 slack_inbox를 폴링해
세션 턴을 claim 하고, 코어(handle_message)로 넘긴 뒤 결과를 slack_outbox에
INSERT 한다(실제 슬랙 발신은 slack_bridge.py 센더가 담당).

내구성 의미론 (at-least-once):
- 코어 handle_message는 백그라운드 태스크를 스폰하고 즉시 리턴하므로,
  done/error 마킹과 세션 락 해제는 코어의 처리완료 훅
  on_processing_complete(SUCCESS/FAILURE/CANCELLED)에서 수행한다.
  즉 done = "에이전트 런 + 응답 발신까지 끝남"이다.
- 처리 중 프로세스가 죽으면 row는 'claimed'로 남고, 폴링 루프의 주기적
  reclaim(reclaim_stale_claimed)이 CLAIM_TTL 경과 후 pending으로 복구한다.
  따라서 재시작 시 유실 대신 재처리(드물게 중복 응답 가능)가 일어난다.

필수 env: QUEUE_DB_PATH / QUEUE_AGENT / QUEUE_REPO_ROOT
옵션 env: QUEUE_POLL_INTERVAL(기본 2.0초, 최소 0.2초),
         QUEUE_ALLOWED_SENDERS(콤마 구분), QUEUE_ALLOW_ALL_USERS
"""

import asyncio
import logging
import os
import re
import sys
import time
import uuid
from typing import Any, Dict, Optional

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    ProcessingOutcome,
    SendResult,
)
from gateway.config import Platform, PlatformConfig

logger = logging.getLogger(__name__)

# done/error 마킹이 에이전트 런 전체(응답 발신 포함)를 커버하므로, TTL이 짧으면
# 장시간 턴 도중 락 만료·reclaim으로 중복 실행이 난다 — 600s에서 상향.
CLAIM_TTL_SECONDS = 1800
# reclaim은 매 폴링이 아니라 이 간격으로 throttle(러너와 동일 패턴).
RECLAIM_INTERVAL_SECONDS = 60
# 0/음수 폴링 간격은 공유 라이브 SQLite(slack_bridge와 공유) 상대 busy-loop이
# 되므로 하한을 강제한다.
MIN_POLL_INTERVAL_SECONDS = 0.2

# send() 라우팅 접두 규약.
#   "queue:<slack채널>"  → 자동 응답(코어가 인바운드 턴에 답) → slack_outbox.
#   그 외(에이전트 키)    → 아웃바운드 handoff → 상대 target의 slack_inbox.
# 인바운드 턴은 build_source에서 이 접두로 정규화되므로(_dispatch_turn) 자기
# 채널로의 응답이 handoff로 오분류되지 않는다. slack_bridge 센더는 raw 슬랙
# 채널만 발송 허용(allowed_channel_ids 게이트)하므로 outbox 삽입 직전 접두를 벗긴다.
_REPLY_CHANNEL_PREFIX = "queue:"
# 아웃바운드 handoff의 합성 event_ts/thread_ts 접두(슬랙 ts와 충돌 없는 고유값).
_HANDOFF_EVENT_PREFIX = "qho-"
# raw 슬랙 채널ID(C/G/D + 영숫자) 패턴 — 에이전트 키가 아니라 채널ID가 큐
# handoff target으로 잘못 넘어온 경우를 식별한다. 에이전트 키는 소문자라
# (chami/chadol/mei/anna/jeff) 이 대문자 접두 패턴에 매치되지 않는다.
_SLACK_CHANNEL_ID_RE = re.compile(r"^[CGD][A-Z0-9]{7,}$")


def _normalize_reply_channel(channel: str) -> str:
    """인바운드 채널을 자동응답용 'queue:<채널>' 형태로 정규화(멱등)."""
    channel = channel or ""
    if channel.startswith(_REPLY_CHANNEL_PREFIX):
        return channel
    return _REPLY_CHANNEL_PREFIX + channel


def _route_and_insert(repo, agent: str, target: str, content: str, thread_hint) -> Dict[str, Any]:
    """큐 send 라우팅(blocking) — send()와 _standalone_send() 공용.

    - target이 'queue:' 접두면 자동 응답 → insert_outbox(raw 채널로 접두 제거).
    - 그 외(에이전트 키) → 아웃바운드 handoff → insert_inbox(target=상대).

    반환은 standalone_sender_fn 계약(dict)과 동일: {"success": True, "message_id": ...}
    또는 {"error": str}. 방어: 빈 target·자기 자신 target·raw 슬랙 채널ID는
    삽입 없이 에러.

    ⚠️ 발신자 인가 결합(숨은 전제): handoff row는 slack_user_id=<발신 에이전트
    키>(예 'chami')로 박힌다. 수신 에이전트의 코어 authz는 default-deny라,
    수신측이 자기 QUEUE_ALLOWED_SENDERS에 이 발신 에이전트 키를 넣거나
    QUEUE_ALLOW_ALL_USERS를 켜지 않으면 handoff가 "sender not allowed"로 error
    마킹되고 소실된다. 즉 여기서 success=True가 나도 수신측 게이팅에 따라 조용히
    버려질 수 있다(코드 우회는 M2 — 지금은 이 전제만 문서화). """
    target = (target or "").strip()
    if not target:
        return {"error": "empty target"}

    if target.startswith(_REPLY_CHANNEL_PREFIX):
        channel = target[len(_REPLY_CHANNEL_PREFIX):]
        row_id = repo.insert_outbox(
            channel_id=channel,
            thread_ts=str(thread_hint or ""),
            text=content,
            created_by=f"queue:{agent}",
        )
        return {"success": True, "message_id": str(row_id)}

    if target == agent:
        # 자기 자신에게 handoff = 무한 루프 위험 → 삽입 없이 거부.
        return {"error": "cannot handoff to self"}

    if _SLACK_CHANNEL_ID_RE.match(target):
        # raw 슬랙 채널ID('C0B69KP8G2J' 등)는 handoff target(에이전트 키)이 아니다.
        # 그대로 insert하면 아무 워커도 처리 못 하는 죽은 row가 생기고, 발신자에겐
        # success로 보여 실패가 은폐된다. 자동 응답이라면 'queue:<채널>' 접두를
        # 써야 outbox로 간다 → 여기선 삽입 없이 명시적으로 거부한다.
        return {"error": "looks like a raw slack channel id, not a queue handoff target"}

    event_ts = f"{_HANDOFF_EVENT_PREFIX}{uuid.uuid4()}"
    thread_ts = str(thread_hint) if thread_hint else f"{_HANDOFF_EVENT_PREFIX}{uuid.uuid4()}"
    inserted = repo.insert_inbox(
        slack_event_ts=event_ts,
        channel_id=f"{_REPLY_CHANNEL_PREFIX}handoff:{agent}->{target}",
        thread_ts=thread_ts,
        slack_user_id=agent,
        text=content,
        target=target,
    )
    if not inserted:
        # event_ts는 row 식별자일 뿐이며 매 send마다 새 uuid라, 재시도 시엔 새
        # event_ts가 생겨 이 UNIQUE 충돌 분기는 사실상 안 탄다(= at-least-once,
        # 재시도가 중복 handoff를 만들 수 있음). 따라서 이 분기는 "재시도 흡수"가
        # 아니라 동일 event_ts를 두 번 넣는 드문 경우(테스트·호출자 uuid 고정)만
        # 방어한다. 진짜 idempotency는 M2에서 검토.
        return {"error": "duplicate handoff"}
    return {"success": True, "message_id": event_ts}


def check_queue_requirements() -> bool:
    """Queue 어댑터는 Python stdlib(sqlite3)만 사용한다 — 추가 의존성 없음."""
    return True


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


class QueueAdapter(BasePlatformAdapter):
    """slack_agent 로컬 SQLite 큐를 폴링하는 게이트웨이 어댑터."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("queue"))

        extra = config.extra or {}
        self._db_path = (os.getenv("QUEUE_DB_PATH", "") or str(extra.get("db_path", ""))).strip()
        self._agent = (os.getenv("QUEUE_AGENT", "") or str(extra.get("agent", ""))).strip()
        self._repo_root = (os.getenv("QUEUE_REPO_ROOT", "") or str(extra.get("repo_root", ""))).strip()
        self._poll_interval = max(
            MIN_POLL_INTERVAL_SECONDS, _float_env("QUEUE_POLL_INTERVAL", 2.0)
        )
        # pid 접미: 같은 agent로 이중 기동돼도 release_session_lock의
        # locked_by=worker 가드가 상대 프로세스의 락을 풀지 않게.
        self._worker = f"queue-adapter-{self._agent}-{os.getpid()}"
        self._repo = None
        self._poll_task: Optional[asyncio.Task] = None
        # 코어로 인계된 in-flight 턴: event.message_id(slack_event_ts) -> SessionTurn.
        # on_processing_complete에서 pop해 done/error 마킹 + 세션 락 해제.
        self._inflight: Dict[str, Any] = {}
        self._last_reclaim = 0.0

        logger.info(
            "[Queue] Adapter initialized (db=%s, agent=%s)", self._db_path, self._agent
        )

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """필수 설정 검증 후 bridge repo를 열고 폴링 태스크를 시작한다."""
        missing = [
            name
            for name, value in (
                ("QUEUE_DB_PATH", self._db_path),
                ("QUEUE_AGENT", self._agent),
                ("QUEUE_REPO_ROOT", self._repo_root),
            )
            if not value
        ]
        if missing:
            message = (
                "Not configured — missing "
                + ", ".join(missing)
                + ". Set the QUEUE_* env vars (or platforms.queue in config.yaml)."
            )
            logger.error("[Queue] %s", message)
            # 설정 누락은 non-retryable — 빈 설정 상대로 무한 재접속을 막는다.
            self._set_fatal_error(
                "queue_missing_configuration", message, retryable=False
            )
            return False

        # bridge 패키지(slack_agent 레포)를 import 가능하게 — idempotent.
        # append(insert(0) 금지): slack_agent 루트엔 최상위 config.py(시크릿) 등
        # 흔한 이름의 모듈이 있어, 최우선 경로로 두면 향후 어떤 코드의 bare
        # `import config` 한 줄로 시크릿 모듈이 조용히 로드된다.
        if self._repo_root not in sys.path:
            sys.path.append(self._repo_root)
        try:
            def _load_repo():
                from bridge.local_repo import SQLiteQueueRepo

                return SQLiteQueueRepo(self._db_path)

            # 생성자가 sqlite connect + DDL(blocking, busy 시 최대 30초)을
            # 수행하므로 이벤트루프 밖(스레드)에서 만든다.
            self._repo = await asyncio.to_thread(_load_repo)
        except Exception as e:
            message = (
                f"Failed to load slack_agent bridge repo "
                f"(QUEUE_REPO_ROOT={self._repo_root}, QUEUE_DB_PATH={self._db_path}): {e}"
            )
            logger.error("[Queue] %s", message, exc_info=True)
            self._set_fatal_error("queue_repo_import_failed", message, retryable=False)
            return False

        self._mark_connected()
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info(
            "[Queue] Connected — polling %s as target '%s' (interval %.2fs)",
            self._db_path, self._agent, self._poll_interval,
        )
        return True

    async def _poll_loop(self) -> None:
        """상시 폴링 루프 — 어떤 예외에도 죽지 않는다(최외곽 가드)."""
        while self._running:
            processed = False
            try:
                await self._maybe_reclaim()
                processed = await self._poll_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[Queue] Poll error: %s", e, exc_info=True)
            if not processed:
                await asyncio.sleep(self._poll_interval)

    async def _maybe_reclaim(self) -> None:
        """크래시/취소로 'claimed'에 고착된 row를 pending으로 복구한다.

        claim과 done/error 마킹 사이에 프로세스가 죽으면 아무도 되돌리지
        않으므로(러너의 reaper는 자기 target만 회수) 여기서 주기적으로
        재수거한다. 기준은 세션 락 TTL과 동일(CLAIM_TTL_SECONDS) — 락이
        만료된 row만 되돌아오므로 정상 처리 중인 턴은 건드리지 않는다.
        """
        now = time.monotonic()
        if self._last_reclaim and now - self._last_reclaim < RECLAIM_INTERVAL_SECONDS:
            return
        self._last_reclaim = now
        recovered = await asyncio.to_thread(
            self._repo.reclaim_stale_claimed,
            target=self._agent,
            older_than_seconds=CLAIM_TTL_SECONDS,
        )
        if recovered:
            logger.warning(
                "[Queue] Reclaimed %d stale claimed row(s) (older than %ds)",
                recovered, CLAIM_TTL_SECONDS,
            )

    async def _poll_once(self) -> bool:
        """큐에서 세션 턴 하나를 claim해 처리한다. 처리했으면 True.

        SQLiteQueueRepo는 동기 blocking이므로 모든 repo 호출은 asyncio.to_thread
        로 스레드에 내린다(이벤트루프 블로킹 금지).
        """
        turn = await asyncio.to_thread(
            self._repo.claim_session_turn,
            target=self._agent,
            worker=self._worker,
            ttl_seconds=CLAIM_TTL_SECONDS,
        )
        if turn is None:
            return False
        handed_off = False
        try:
            handed_off = await self._process_turn(turn)
        finally:
            # 코어로 인계되지 못한 턴(발신자 불허·디스패치 실패·취소)만 여기서
            # 즉시 락 해제. 인계된 턴은 on_processing_complete가 done/error
            # 마킹과 함께 해제한다 — 그동안 같은 세션의 후속 턴은 SQLite
            # pending에 내구성 있게 대기한다(인메모리 busy 큐로 안 흘러감).
            if not handed_off:
                try:
                    await asyncio.to_thread(
                        self._repo.release_session_lock,
                        session_id=turn.session_id,
                        worker=self._worker,
                    )
                except Exception:
                    logger.exception(
                        "[Queue] Failed to release session lock (session_id=%s)",
                        turn.session_id,
                    )
        return True

    async def _process_turn(self, turn) -> bool:
        """claim된 턴 하나를 코어로 인계한다. 인계 성공 시 True.

        예외는 여기서 삼키고 row를 error로 마킹한다 — 폴링 루프는 절대 죽지 않는다.
        """
        try:
            return await self._dispatch_turn(turn)
        except Exception as e:
            logger.error(
                "[Queue] Turn processing failed (inbox_id=%s): %s",
                turn.inbox_id, e, exc_info=True,
            )
            try:
                await asyncio.to_thread(
                    self._repo.mark_inbox_error, turn.inbox_id, str(e)[:500]
                )
            except Exception:
                logger.exception(
                    "[Queue] Failed to mark inbox error (inbox_id=%s)", turn.inbox_id
                )
            return False

    @staticmethod
    def _sender_allowed(slack_user_id: str) -> bool:
        """QUEUE_ALLOWED_SENDERS 조기 차단 가드(email 어댑터 관례).

        미설정이면 어댑터 레벨에서는 전부 통과 — 진짜 인가는 코어 authz가
        QUEUE_ALLOWED_SENDERS / QUEUE_ALLOW_ALL_USERS / 전역 GATEWAY_*로
        default-deny 판정한다(이중 게이트).
        """
        allowed_raw = os.getenv("QUEUE_ALLOWED_SENDERS", "").strip()
        if not allowed_raw:
            return True
        allowed = {uid.strip() for uid in allowed_raw.split(",") if uid.strip()}
        return slack_user_id in allowed

    async def _dispatch_turn(self, turn) -> bool:
        """턴을 MessageEvent로 변환해 코어로 넘긴다. 인계했으면 True.

        핵심: 코어 handle_message는 백그라운드 태스크를 스폰하고 즉시 리턴하는
        fire-and-forget이다. 따라서 여기서 done을 찍지 않고, in-flight 테이블에
        등록한 뒤 처리완료 훅(on_processing_complete)에서 마킹한다. 훅이 끝내
        오지 않는 비정상 경로는 CLAIM_TTL 경과 후 reclaim이 pending으로 복구.
        """
        if not self._sender_allowed(turn.slack_user_id):
            logger.warning(
                "[Queue] Dropping non-allowlisted sender: %s (inbox_id=%s)",
                turn.slack_user_id, turn.inbox_id,
            )
            await asyncio.to_thread(
                self._repo.mark_inbox_error, turn.inbox_id, "sender not allowed"
            )
            return False
        # 접두 규약 방어: 자동 응답이 handoff로 오분류되지 않게 채널을 'queue:'로
        # 정규화한다(멱등). send()가 이 접두를 보고 outbox로 라우팅하며, 삽입
        # 직전 접두를 벗겨 raw 슬랙 채널로 발송한다.
        reply_channel = _normalize_reply_channel(turn.channel_id)
        source = self.build_source(
            chat_id=reply_channel,
            chat_name=reply_channel,
            chat_type="channel",
            user_id=turn.slack_user_id,
            user_name=turn.slack_user_id,
            thread_id=turn.thread_ts,
        )
        event = MessageEvent(
            text=turn.text,
            source=source,
            message_id=turn.slack_event_ts,
        )
        logger.info(
            "[Queue] New turn from %s in %s (inbox_id=%s)",
            turn.slack_user_id, turn.channel_id, turn.inbox_id,
        )
        self._inflight[event.message_id] = turn
        try:
            await self.handle_message(event)
        except BaseException:
            # 동기 실패(스폰 전) — in-flight 등록을 되돌리고 상위에서 error 마킹.
            self._inflight.pop(event.message_id, None)
            raise
        return True

    async def on_processing_complete(
        self, event: MessageEvent, outcome: ProcessingOutcome
    ) -> None:
        """코어 백그라운드 처리 완료 훅 — 여기서 비로소 done/error + 락 해제.

        SUCCESS = 에이전트 런과 응답 발신까지 끝남 -> done.
        FAILURE/CANCELLED -> error(유실 아님 — 상태로 가시화).
        """
        turn = self._inflight.pop(event.message_id, None) if event.message_id else None
        if turn is None:
            return
        try:
            if outcome is ProcessingOutcome.SUCCESS:
                await asyncio.to_thread(self._repo.mark_inbox_done, turn.inbox_id)
            else:
                await asyncio.to_thread(
                    self._repo.mark_inbox_error,
                    turn.inbox_id,
                    f"processing {outcome.value}",
                )
        except Exception:
            logger.exception(
                "[Queue] Failed to mark inbox %s (inbox_id=%s)",
                outcome.value, turn.inbox_id,
            )
        finally:
            try:
                await asyncio.to_thread(
                    self._repo.release_session_lock,
                    session_id=turn.session_id,
                    worker=self._worker,
                )
            except Exception:
                logger.exception(
                    "[Queue] Failed to release session lock (session_id=%s)",
                    turn.session_id,
                )

    async def disconnect(self) -> None:
        """폴링을 멈추고 태스크를 정리한다."""
        self._mark_disconnected()
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        logger.info("[Queue] Disconnected.")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """큐로 메시지를 보낸다 — chat_id 접두로 두 경로 분기(_route_and_insert).

        - 'queue:<채널>'  → 자동 응답: slack_outbox INSERT(발신은 브리지 센더).
        - 그 외(에이전트) → 아웃바운드 handoff: 상대 target의 slack_inbox INSERT.

        thread_ts는 코어가 넣어주는 metadata["thread_id"](= source.thread_id)
        우선, 없으면 reply_to(트리거 메시지 ts)로 폴백한다.
        """
        if self._repo is None:
            return SendResult(success=False, error="queue repo not connected")
        thread_hint = (metadata.get("thread_id") if metadata else None) or reply_to
        try:
            result = await asyncio.to_thread(
                _route_and_insert, self._repo, self._agent, chat_id, content, thread_hint
            )
        except Exception as e:
            logger.error("[Queue] Send failed to %s: %s", chat_id, e)
            return SendResult(success=False, error=str(e))
        if result.get("success"):
            return SendResult(success=True, message_id=result.get("message_id"))
        return SendResult(success=False, error=result.get("error", "send failed"))

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """슬랙 채널 큐 대화의 기본 정보."""
        return {"name": chat_id, "type": "channel", "chat_id": chat_id}


def _is_connected(config) -> bool:
    """세 필수 설정(db_path/agent/repo_root)이 모두 있어야 활성으로 판정."""
    extra = getattr(config, "extra", {}) or {}

    def _value(extra_key: str, env_name: str) -> str:
        raw = extra.get(extra_key)
        if raw:
            return str(raw).strip()
        import hermes_cli.gateway as gateway_mod

        return (gateway_mod.get_env_value(env_name) or "").strip()

    return all(
        _value(key, env)
        for key, env in (
            ("db_path", "QUEUE_DB_PATH"),
            ("agent", "QUEUE_AGENT"),
            ("repo_root", "QUEUE_REPO_ROOT"),
        )
    )


def _build_adapter(config):
    """PlatformConfig로부터 QueueAdapter를 만드는 팩토리."""
    return QueueAdapter(config)


async def _standalone_send(
    pconfig,
    chat_id,
    message,
    *,
    thread_id=None,
    media_files=None,
    force_document=False,
):
    """게이트웨이 없이(out-of-process) 큐로 보내는 one-shot 전송.

    standalone_sender_fn 계약(email 어댑터와 동일 시그니처·반환)을 구현한다.
    cron/러너가 게이트웨이와 다른 프로세스로 돌 때 send_message가 이 경로로
    떨어진다. QUEUE_DB_PATH/QUEUE_AGENT/QUEUE_REPO_ROOT는 pconfig.extra 우선,
    없으면 os.getenv 폴백. send()와 동일한 라우팅(_route_and_insert)을 쓴다.

    ⚠️ 아웃바운드 handoff의 발신자 인가 결합: 여기서 만드는 handoff row는
    slack_user_id=<발신 에이전트 키>로 박히므로, 수신 에이전트가 자기
    QUEUE_ALLOWED_SENDERS에 이 발신 에이전트 키를 넣거나 QUEUE_ALLOW_ALL_USERS를
    켜야 handoff가 처리된다. 아니면 수신측 코어 authz(default-deny)가
    "sender not allowed"로 error 마킹해 소실시킨다(상세: _route_and_insert).
    """
    extra = getattr(pconfig, "extra", {}) or {}

    def _cfg(extra_key: str, env_name: str) -> str:
        raw = extra.get(extra_key)
        if raw:
            return str(raw).strip()
        return os.getenv(env_name, "").strip()

    db_path = _cfg("db_path", "QUEUE_DB_PATH")
    agent = _cfg("agent", "QUEUE_AGENT")
    repo_root = _cfg("repo_root", "QUEUE_REPO_ROOT")
    if not all([db_path, agent, repo_root]):
        return {
            "error": "Queue not configured "
            "(QUEUE_DB_PATH, QUEUE_AGENT, QUEUE_REPO_ROOT required)"
        }

    # append(insert(0) 금지): slack_agent 루트의 최상위 config.py(시크릿) 등이
    # 전역 최우선 import가 되지 않게 — 어댑터 본체와 동일 규칙.
    if repo_root not in sys.path:
        sys.path.append(repo_root)

    def _do():
        from bridge.local_repo import SQLiteQueueRepo

        repo = SQLiteQueueRepo(db_path)
        return _route_and_insert(repo, agent, chat_id, message, thread_id)

    try:
        return await asyncio.to_thread(_do)
    except Exception as e:
        logger.error("[Queue] Standalone send failed to %s: %s", chat_id, e)
        return {"error": f"Queue send failed: {e}"}


def register(ctx) -> None:
    """플러그인 진입점 — Hermes 플러그인 시스템이 호출한다."""
    ctx.register_platform(
        name="queue",
        label="Queue",
        adapter_factory=_build_adapter,
        check_fn=check_queue_requirements,
        is_connected=_is_connected,
        required_env=["QUEUE_DB_PATH", "QUEUE_AGENT", "QUEUE_REPO_ROOT"],
        install_hint="Queue uses the Python stdlib (sqlite3 via slack_agent bridge) — no extra deps",
        allowed_users_env="QUEUE_ALLOWED_SENDERS",
        allow_all_env="QUEUE_ALLOW_ALL_USERS",
        standalone_sender_fn=_standalone_send,
        max_message_length=40_000,
        emoji="📬",
    )
