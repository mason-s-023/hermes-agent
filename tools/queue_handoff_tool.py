"""queue_handoff — 헤르메스 에이전트 간 DB 큐 handoff 도구(agent-callable).

현재 에이전트가 **다른 헤르메스 에이전트**(차미·차돌·메이·안나·제프)에게 작업을
넘길 때 쓴다. slack_agent 로컬 SQLite 큐(bridge.local_repo.SQLiteQueueRepo)의
slack_inbox에 ``target=<상대 에이전트>``, ``slack_user_id=<자기 에이전트>``로
INSERT 하면 상대 워커(큐 어댑터)가 claim 해서 처리한다.

send_message와의 차이: **외부 플랫폼(슬랙/텔레그램 등) 발신이 아니다.** 순수하게
큐 내부 에이전트 라우팅만 한다. 외부 채널로 보내려면 send_message/자동 응답 경로를
써야 하며, raw 슬랙 채널ID를 여기에 넘기면 거부된다.

발신자 정체성 = ``QUEUE_AGENT`` env(프로세스 고정). 현재 대화 스레드 =
``HERMES_SESSION_THREAD_ID``(있으면 이어감).

필수 env: QUEUE_AGENT / QUEUE_REPO_ROOT, 그리고 QUEUE_DB_PATH 또는 QUEUE_ENDPOINT.
선택 env: QUEUE_TOKEN, QUEUE_KNOWN_AGENTS(콤마 구분 — 기본 로스터 chami/chadol/mei/anna/jeff
         밖의 에이전트를 handoff 대상으로 허용).

⚠️ 수신측 인가(숨은 전제): handoff row는 slack_user_id=<발신 에이전트 키>로 박히고,
수신 에이전트 코어 authz는 default-deny다. 수신측이 자기 QUEUE_ALLOWED_SENDERS에
발신 키를 넣거나 QUEUE_ALLOW_ALL_USERS를 켜지 않으면 handoff가 "sender not allowed"로
error 마킹돼 소실된다 — 이 도구가 success를 반환해도 그렇다(인가 우회는 별도 이슈).
"""

import asyncio
import os
import re
import sys
import uuid

from tools.registry import registry, tool_error, tool_result

# raw 슬랙 채널ID(C/G/D + 영숫자) 패턴 — 에이전트 키(소문자)가 아니라 채널ID가
# 잘못 넘어온 경우를 식별해 거부한다(어댑터 _route_and_insert와 동일 규칙).
_SLACK_CHANNEL_ID_RE = re.compile(r"^[CGD][A-Z0-9]{7,}$")

# 아웃바운드 handoff의 합성 event_ts/thread_ts 접두(슬랙 ts와 충돌 없는 고유값).
_HANDOFF_EVENT_PREFIX = "qh-"

# 알려진 헤르메스 에이전트 키(소문자 canonical). LLM이 `to`를 직접 채우는
# 표면이라 오타·환각('chadl'·'claude')이 현실적 실패 모드다. 화이트리스트로
# 걸러 아무 워커도 claim 못 하는 죽은 pending row가 생기는 걸 막는다. 운영에서
# 로스터가 늘면 QUEUE_KNOWN_AGENTS(콤마 구분)로 덮어쓴다.
_KNOWN_AGENTS_DEFAULT = ("chami", "chadol", "mei", "anna", "jeff")

# insert_inbox는 어댑터 send()를 우회하므로 플랫폼 max_message_length(register의
# 40_000) 가드가 적용되지 않는다 — 여기서 동일 상한을 직접 강제한다.
_MAX_MESSAGE_LENGTH = 40_000


def _env(name: str) -> str:
    return (os.getenv(name, "") or "").strip()


def _known_agents() -> set:
    """허용 에이전트 키 집합(casefold). QUEUE_KNOWN_AGENTS로 덮어쓸 수 있다."""
    raw = _env("QUEUE_KNOWN_AGENTS")
    keys = raw.split(",") if raw else list(_KNOWN_AGENTS_DEFAULT)
    return {k.strip().casefold() for k in keys if k.strip()}


def check_queue_handoff() -> bool:
    """큐가 활성(필수 env + local db 또는 HTTP endpoint)일 때만 도구를 노출한다."""
    return bool(
        _env("QUEUE_AGENT")
        and _env("QUEUE_REPO_ROOT")
        and (_env("QUEUE_DB_PATH") or _env("QUEUE_ENDPOINT"))
    )


def _do_insert(
    repo_root: str,
    db_path: str,
    *,
    endpoint: str,
    token: str,
    event_ts: str,
    channel_id: str,
    thread_ts: str,
    agent: str,
    message: str,
    target: str,
) -> bool:
    """blocking 큐 삽입 — asyncio.to_thread로 감싸 호출한다.

    QUEUE_REPO_ROOT를 sys.path에 **append**(insert(0) 금지 — slack_agent 루트의
    config.py 시크릿이 전역 최우선 import 되는 걸 막는다)한 뒤 bridge 패키지를
    import 한다.
    """
    if repo_root and repo_root not in sys.path:
        sys.path.append(repo_root)
    from bridge.agent_repo import make_agent_queue_repo

    repo = make_agent_queue_repo(db_path=db_path, endpoint=endpoint, token=token)
    return repo.insert_inbox(
        slack_event_ts=event_ts,
        channel_id=channel_id,
        thread_ts=thread_ts,
        slack_user_id=agent,
        text=message,
        target=target,
    )


async def queue_handoff_tool(args: dict, **kw) -> str:
    """다른 헤르메스 에이전트에게 큐 handoff. 반환 = JSON 문자열."""
    agent = _env("QUEUE_AGENT")
    if not agent:
        return tool_error("QUEUE_AGENT is not set — cannot identify sender agent")

    db_path = _env("QUEUE_DB_PATH")
    endpoint = _env("QUEUE_ENDPOINT")
    token = _env("QUEUE_TOKEN")
    repo_root = _env("QUEUE_REPO_ROOT")
    if not repo_root or (not db_path and not endpoint):
        return tool_error(
            "queue is not configured (QUEUE_REPO_ROOT and QUEUE_DB_PATH or QUEUE_ENDPOINT missing)"
        )

    to = ((args or {}).get("to") or "").strip()
    message = (args or {}).get("message")

    # --- 방어(삽입 없이 거부) ---
    if not to:
        return tool_error("empty target")
    if not (message and str(message).strip()):
        return tool_error("empty message")
    if _SLACK_CHANNEL_ID_RE.match(to):
        return tool_error(
            "looks like a raw slack channel id, not a queue handoff target "
            "(pass an agent key like 'chadol', not 'C0B69KP8G2J')"
        )
    # 대소문자 무시 self-check + canonical화: 'Chami'가 self 가드를 우회해
    # 아무도 claim 못 하는 죽은 row가 되는 걸 막는다(claim은 소문자 target 정확일치).
    to = to.casefold()
    if to == agent.casefold():
        return tool_error("cannot handoff to self")
    # 알려진 에이전트 키만 허용 — 오타·환각이 pending으로 영구 잔류하며
    # success로 위장되는 걸 막는다.
    known = _known_agents()
    if to not in known:
        return tool_error(
            f"unknown agent key '{to}' — known agents: {sorted(known)} "
            "(set QUEUE_KNOWN_AGENTS to extend the roster)"
        )

    message = str(message)
    if len(message) > _MAX_MESSAGE_LENGTH:
        return tool_error(
            f"message too long ({len(message)} chars > {_MAX_MESSAGE_LENGTH})"
        )

    # 스레드: 명시 인자 > 현재 세션 스레드 > 새 합성 ts.
    # HERMES_SESSION_CHAT_ID(채널 식별자)는 thread_ts 폴백에서 제외한다 —
    # 채널ID를 thread_ts로 쓰면 같은 채널에서 발생한 서로 무관한 handoff들이
    # 수신측 한 세션으로 뭉쳐 격리가 깨진다(session_context.py: CHAT_ID=채널).
    from gateway.session_context import get_session_env

    thread_ts = (
        str((args or {}).get("thread") or "").strip()
        or get_session_env("HERMES_SESSION_THREAD_ID", "")
        or f"{_HANDOFF_EVENT_PREFIX}{uuid.uuid4()}"
    )
    event_ts = f"{_HANDOFF_EVENT_PREFIX}{uuid.uuid4()}"
    channel_id = f"queue:handoff:{agent}->{to}"

    try:
        inserted = await asyncio.to_thread(
            _do_insert,
            repo_root,
            db_path,
            endpoint=endpoint,
            token=token,
            event_ts=event_ts,
            channel_id=channel_id,
            thread_ts=thread_ts,
            agent=agent,
            message=message,
            target=to,
        )
    except Exception as exc:  # noqa: BLE001 — 어떤 실패든 도구 계약(JSON error)로 변환
        return tool_error(f"handoff insert failed: {exc}")

    if not inserted:
        # 매 호출 새 uuid event_ts라 UNIQUE 충돌은 사실상 안 남 — 드문 경우만 방어.
        return tool_error("duplicate handoff (event_ts collision)")

    # success = "큐에 적재 성공"이지 "수신 처리 확정"이 아니다. 수신측 코어
    # authz는 default-deny라, 발신 키가 수신측 QUEUE_ALLOWED_SENDERS에 없고
    # QUEUE_ALLOW_ALL_USERS도 꺼져 있으면 조용히 드롭될 수 있다 → note로 명시해
    # 호출 LLM이 success를 "배달 완료"로 오인하지 않게 한다.
    return tool_result(
        success=True,
        target=to,
        message_id=event_ts,
        thread_ts=thread_ts,
        delivery="enqueued",
        note=(
            "enqueued to the target's queue; actual processing depends on the "
            "receiver accepting this sender (QUEUE_ALLOWED_SENDERS / "
            "QUEUE_ALLOW_ALL_USERS)"
        ),
    )


QUEUE_HANDOFF_SCHEMA = {
    "name": "queue_handoff",
    "description": (
        "Hand off work to ANOTHER Hermes agent (chami, chadol, mei, anna, jeff) "
        "via the internal DB queue. This does NOT send a message to any external "
        "platform (Slack/Telegram/etc.) — it enqueues a task that the target "
        "agent's worker will claim and process.\n\n"
        "Use when you want a different agent to take over a task (e.g. delegate a "
        "coding/deploy task to chadol). Pass the target agent's key in `to` "
        "(lowercase, e.g. 'chadol') — NOT a raw Slack channel id. You are "
        "identified as the sender automatically."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "to": {
                "type": "string",
                "description": (
                    "Target agent key (lowercase): one of chami/chadol/mei/anna/jeff. "
                    "Must not be yourself and must not be a raw Slack channel id."
                ),
            },
            "message": {
                "type": "string",
                "description": (
                    "The handoff content — the task/instruction for the target "
                    "agent. Include enough context to act without this conversation."
                ),
            },
            "thread": {
                "type": "string",
                "description": (
                    "Optional. Existing thread_ts to thread this handoff under. "
                    "Omit to derive from the current session or start fresh. "
                    "Note: this is a one-way enqueue — the target agent does NOT "
                    "automatically reply back to you on this thread."
                ),
            },
        },
        "required": ["to", "message"],
    },
}


# --- Registry (모듈 최상위 등록 → discover_builtin_tools가 AST로 자동 발견·import) ---
registry.register(
    name="queue_handoff",
    toolset="queue",  # hermes-queue 플러그인-플랫폼 번들에 자동 편입(toolset=="queue")
    schema=QUEUE_HANDOFF_SCHEMA,
    handler=lambda args, **kw: queue_handoff_tool(args, **kw),  # 코루틴 반환 → is_async
    check_fn=check_queue_handoff,
    is_async=True,
    description="현재 에이전트가 다른 헤르메스 에이전트에게 DB 큐로 handoff. 외부 플랫폼 발신 아님.",
    emoji="📬",
)
