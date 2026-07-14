"""Queue 플랫폼 어댑터 테스트.

slack_agent 로컬 SQLite 큐(bridge.local_repo.SQLiteQueueRepo)를 실물로 사용한다
(tmp 파일 DB). sys.path 조작(QUEUE_REPO_ROOT 삽입)은 이 픽스처에서 수행한다.

의미론(어댑터 계약): 코어 handle_message는 fire-and-forget이므로 done/error
마킹과 세션 락 해제는 on_processing_complete 훅에서 일어난다. 그 전까지
row는 'claimed'다(재시작 시 reclaim으로 복구 — at-least-once).

커버:
T1 connect: 필수 env 각각 누락 시 False + fatal
T2 poll: pending row(target 일치) -> handler 1회 호출 + 이벤트 필드 매핑
T3 인계 직후엔 claimed 유지 -> 훅 SUCCESS에서 done + 락 해제
T4 훅 FAILURE -> error / handle_message 동기 예외 -> error + 루프 생존
T5 (negative) target 불일치 row 미처리(pending 유지)
T6 (negative) QUEUE_ALLOWED_SENDERS 불허 발신자 -> handler 미호출 + row error
T7 send -> slack_outbox row 생성(channel/thread/text/created_by)
T8 disconnect -> 폴링 태스크 종료
T9 reclaim: 죽은 워커의 stale claimed row -> pending 복구 -> 재처리 가능
T10 통합: 진짜 handle_message 파이프라인(핸들러 -> send -> 훅) -> done + outbox
"""

import asyncio
import os
import sqlite3
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

# slack_agent 레포 루트(bridge 패키지 제공). 라이브 기본값은 스미스 로컬 체크아웃.
SLACK_AGENT_ROOT = os.environ.get(
    "QUEUE_TEST_REPO_ROOT", "/Users/charde023/workspace/slack_agent"
)
_HAS_SLACK_AGENT = Path(SLACK_AGENT_ROOT, "bridge", "local_repo.py").is_file()

if _HAS_SLACK_AGENT and SLACK_AGENT_ROOT not in sys.path:
    # append — insert(0)은 slack_agent 루트의 config.py(시크릿) 등이 전역
    # 최우선 import가 되는 구조라 금지(어댑터 본체와 동일 규칙).
    sys.path.append(SLACK_AGENT_ROOT)


@unittest.skipUnless(_HAS_SLACK_AGENT, f"slack_agent repo not found: {SLACK_AGENT_ROOT}")
class QueueAdapterTestBase(unittest.TestCase):
    """tmp SQLite + env 픽스처 공통 베이스."""

    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db_path = str(Path(tmpdir.name) / "queue-test.sqlite3")
        self.env = {
            "QUEUE_DB_PATH": self.db_path,
            "QUEUE_AGENT": "chami",
            "QUEUE_REPO_ROOT": SLACK_AGENT_ROOT,
            "QUEUE_POLL_INTERVAL": "0.05",
            # 런타임 상태 파일이 라이브 ~/.hermes를 오염시키지 않게 격리.
            "HERMES_HOME": str(Path(tmpdir.name) / "hermes-home"),
        }
        # 주변 셸/게이트웨이 env가 새어 들어와 위양성을 내지 않게 정리.
        # patch.dict가 시작 시점 환경을 스냅샷하므로 pop해도 teardown에 복원된다.
        env_guard = patch.dict(os.environ, self.env, clear=False)
        env_guard.start()
        self.addCleanup(env_guard.stop)
        os.environ.pop("QUEUE_ALLOWED_SENDERS", None)
        os.environ.pop("QUEUE_ALLOW_ALL_USERS", None)

    def make_adapter(self, env_overrides=None, remove=()):
        env = dict(env_overrides) if env_overrides else {}
        from gateway.config import PlatformConfig
        from plugins.platforms.queue.adapter import QueueAdapter

        with patch.dict(os.environ, env, clear=False):
            for key in remove:
                os.environ.pop(key, None)
            adapter = QueueAdapter(PlatformConfig(enabled=True))
        return adapter

    def make_repo(self):
        from bridge.local_repo import SQLiteQueueRepo

        return SQLiteQueueRepo(self.db_path)

    def inbox_row(self, slack_event_ts):
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        try:
            row = con.execute(
                "SELECT status, error FROM slack_inbox WHERE slack_event_ts = ?",
                (slack_event_ts,),
            ).fetchone()
        finally:
            con.close()
        return row

    @staticmethod
    def outcome(name):
        from gateway.platforms.base import ProcessingOutcome

        return getattr(ProcessingOutcome, name)

    def completing_handle(self, adapter, captured=None):
        """코어 완료(훅 SUCCESS 호출)까지 흉내내는 handle_message 스텁."""

        async def handle(event):
            if captured is not None:
                captured.append(event)
            await adapter.on_processing_complete(event, self.outcome("SUCCESS"))

        return handle


class TestConnectMissingConfig(QueueAdapterTestBase):
    """T1: 필수 env 누락 시 connect()가 False + non-retryable fatal."""

    def _assert_fatal_missing(self, missing_key):
        adapter = self.make_adapter(remove=(missing_key,))
        adapter._message_handler = MagicMock()
        ok = asyncio.run(adapter.connect())
        self.assertFalse(ok)
        self.assertFalse(adapter._running)
        self.assertEqual(adapter._fatal_error_code, "queue_missing_configuration")
        self.assertFalse(adapter._fatal_error_retryable)
        self.assertIn(missing_key, adapter._fatal_error_message)

    def test_missing_db_path(self):
        self._assert_fatal_missing("QUEUE_DB_PATH")

    def test_missing_agent(self):
        self._assert_fatal_missing("QUEUE_AGENT")

    def test_missing_repo_root(self):
        self._assert_fatal_missing("QUEUE_REPO_ROOT")


class TestPollDispatch(QueueAdapterTestBase):
    """T2: pending row(target 일치)를 claim해 handler로 1회 전달 + 필드 매핑."""

    def test_pending_row_dispatched_with_mapped_fields(self):
        repo = self.make_repo()
        repo.insert_inbox(
            slack_event_ts="1700000001.000100",
            channel_id="C0B69KP8G2J",
            thread_ts="1700000000.000001",
            slack_user_id="U0CHAD",
            text="안녕 차미",
            target="chami",
        )

        adapter = self.make_adapter()
        adapter._repo = repo
        captured = []

        async def capture_handle(event):
            captured.append(event)

        adapter.handle_message = capture_handle

        processed = asyncio.run(adapter._poll_once())

        self.assertTrue(processed)
        self.assertEqual(len(captured), 1)
        event = captured[0]
        self.assertEqual(event.text, "안녕 차미")
        self.assertEqual(event.message_id, "1700000001.000100")
        # M1: _dispatch_turn이 채널을 'queue:' 접두로 정규화한다(자동 응답이
        # handoff로 오분류되지 않게). 삽입 시엔 접두를 벗긴 raw 채널을 쓴다.
        self.assertEqual(event.source.chat_id, "queue:C0B69KP8G2J")
        self.assertEqual(event.source.thread_id, "1700000000.000001")
        self.assertEqual(event.source.user_id, "U0CHAD")
        self.assertEqual(event.source.platform.value, "queue")
        # 인계된 턴은 in-flight로 등록돼 훅에서 마감을 기다린다.
        self.assertIn("1700000001.000100", adapter._inflight)


class TestCompletionMarksDone(QueueAdapterTestBase):
    """T3: 인계 직후엔 claimed 유지, 훅 SUCCESS에서 done + 세션 락 해제."""

    def test_done_only_after_processing_complete_hook(self):
        repo = self.make_repo()
        repo.insert_inbox(
            slack_event_ts="1700000002.000100",
            channel_id="C0B69KP8G2J",
            thread_ts="1700000000.000001",
            slack_user_id="U0CHAD",
            text="첫 턴",
            target="chami",
        )

        adapter = self.make_adapter()
        adapter._repo = repo
        captured = []

        async def capture_handle(event):
            captured.append(event)

        adapter.handle_message = capture_handle

        async def scenario():
            self.assertTrue(await adapter._poll_once())
            # 아직 코어가 처리 중 — done이 아니라 claimed여야 한다(내구성 계약).
            self.assertEqual(self.inbox_row("1700000002.000100")["status"], "claimed")
            # 락이 잡혀 있는 동안 같은 세션의 후속 턴은 claim되지 않는다.
            repo.insert_inbox(
                slack_event_ts="1700000003.000100",
                channel_id="C0B69KP8G2J",
                thread_ts="1700000000.000001",
                slack_user_id="U0CHAD",
                text="둘째 턴",
                target="chami",
            )
            self.assertFalse(await adapter._poll_once())

            # 코어 처리 완료 -> done + 락 해제.
            await adapter.on_processing_complete(captured[0], self.outcome("SUCCESS"))
            self.assertEqual(self.inbox_row("1700000002.000100")["status"], "done")
            self.assertNotIn("1700000002.000100", adapter._inflight)

            # 락이 풀렸으니 같은 세션의 다음 턴이 곧바로 claim 가능하다.
            self.assertTrue(await adapter._poll_once())
            await adapter.on_processing_complete(captured[1], self.outcome("SUCCESS"))
            self.assertEqual(self.inbox_row("1700000003.000100")["status"], "done")

        asyncio.run(scenario())


class TestFailureMarking(QueueAdapterTestBase):
    """T4: 훅 FAILURE -> error / handle_message 동기 예외 -> error + 루프 생존."""

    def test_processing_failure_outcome_marks_error(self):
        repo = self.make_repo()
        repo.insert_inbox(
            slack_event_ts="1700000010.000100",
            channel_id="C0B69KP8G2J",
            thread_ts="1700000010.000100",
            slack_user_id="U0CHAD",
            text="터질 턴",
            target="chami",
        )

        adapter = self.make_adapter()
        adapter._repo = repo
        captured = []

        async def capture_handle(event):
            captured.append(event)

        adapter.handle_message = capture_handle

        async def scenario():
            self.assertTrue(await adapter._poll_once())
            await adapter.on_processing_complete(captured[0], self.outcome("FAILURE"))

        asyncio.run(scenario())
        row = self.inbox_row("1700000010.000100")
        self.assertEqual(row["status"], "error")
        self.assertIn("failure", row["error"])

    def test_sync_handle_exception_marks_error_and_keeps_processing(self):
        repo = self.make_repo()
        repo.insert_inbox(
            slack_event_ts="1700000012.000100",
            channel_id="C0B69KP8G2J",
            thread_ts="1700000012.000100",
            slack_user_id="U0CHAD",
            text="터질 턴",
            target="chami",
        )

        adapter = self.make_adapter()
        adapter._repo = repo
        calls = []

        async def boom_handle(event):
            calls.append(event.text)
            raise RuntimeError("boom-테스트")

        adapter.handle_message = boom_handle

        # 예외가 _poll_once 밖으로 전파되지 않아야 폴링 루프가 산다.
        self.assertTrue(asyncio.run(adapter._poll_once()))
        row = self.inbox_row("1700000012.000100")
        self.assertEqual(row["status"], "error")
        self.assertIn("boom-테스트", row["error"])
        # 동기 실패한 턴은 in-flight에 남지 않는다.
        self.assertNotIn("1700000012.000100", adapter._inflight)

        # 다음 row는 정상 처리 — 루프 생존 증명(같은 세션: 락도 풀렸어야 한다).
        repo.insert_inbox(
            slack_event_ts="1700000013.000100",
            channel_id="C0B69KP8G2J",
            thread_ts="1700000012.000100",
            slack_user_id="U0CHAD",
            text="정상 턴",
            target="chami",
        )
        adapter.handle_message = self.completing_handle(adapter, calls)
        self.assertTrue(asyncio.run(adapter._poll_once()))
        self.assertEqual(calls[0], "터질 턴")
        self.assertEqual(self.inbox_row("1700000013.000100")["status"], "done")


class TestTargetMismatch(QueueAdapterTestBase):
    """T5 (negative): target 불일치 row는 건드리지 않는다(pending 유지)."""

    def test_other_target_row_stays_pending(self):
        repo = self.make_repo()
        repo.insert_inbox(
            slack_event_ts="1700000020.000100",
            channel_id="C0B69KP8G2J",
            thread_ts="1700000020.000100",
            slack_user_id="U0CHAD",
            text="차돌 몫",
            target="chadol",
        )

        adapter = self.make_adapter()
        adapter._repo = repo
        adapter._message_handler = MagicMock()

        self.assertFalse(asyncio.run(adapter._poll_once()))
        adapter._message_handler.assert_not_called()
        self.assertEqual(self.inbox_row("1700000020.000100")["status"], "pending")


class TestAllowedSenders(QueueAdapterTestBase):
    """T6 (negative): QUEUE_ALLOWED_SENDERS 설정 시 불허 발신자는 handler 미호출 + error."""

    def test_disallowed_sender_marked_error_without_dispatch(self):
        repo = self.make_repo()
        repo.insert_inbox(
            slack_event_ts="1700000030.000100",
            channel_id="C0B69KP8G2J",
            thread_ts="1700000030.000100",
            slack_user_id="U0INTRUDER",
            text="몰래 온 메시지",
            target="chami",
        )

        adapter = self.make_adapter()
        adapter._repo = repo
        adapter._message_handler = MagicMock()

        with patch.dict(os.environ, {"QUEUE_ALLOWED_SENDERS": "U0CHAD, U0GOOD"}, clear=False):
            self.assertTrue(asyncio.run(adapter._poll_once()))

        adapter._message_handler.assert_not_called()
        row = self.inbox_row("1700000030.000100")
        self.assertEqual(row["status"], "error")
        self.assertIn("sender not allowed", row["error"])

        # 허용 발신자는 통과한다.
        repo.insert_inbox(
            slack_event_ts="1700000031.000100",
            channel_id="C0B69KP8G2J",
            thread_ts="1700000031.000100",
            slack_user_id="U0GOOD",
            text="허용 발신자",
            target="chami",
        )
        captured = []
        adapter.handle_message = self.completing_handle(adapter, captured)
        with patch.dict(os.environ, {"QUEUE_ALLOWED_SENDERS": "U0CHAD, U0GOOD"}, clear=False):
            self.assertTrue(asyncio.run(adapter._poll_once()))
        self.assertEqual(len(captured), 1)
        self.assertEqual(self.inbox_row("1700000031.000100")["status"], "done")


class TestSend(QueueAdapterTestBase):
    """T7: send() -> slack_outbox row 생성(channel/thread/text/created_by 정확)."""

    def outbox_rows(self):
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        try:
            return con.execute(
                "SELECT id, channel_id, thread_ts, text, created_by, status"
                " FROM slack_outbox ORDER BY id"
            ).fetchall()
        finally:
            con.close()

    def test_send_inserts_outbox_row(self):
        adapter = self.make_adapter()
        adapter._repo = self.make_repo()

        # M1: 자동 응답 outbox 경로는 'queue:' 접두 채널로 라우팅된다(접두 없는
        # 이름은 아웃바운드 handoff로 분기). 삽입 시 접두는 벗겨진다.
        result = asyncio.run(
            adapter.send(
                "queue:C0B69KP8G2J",
                "차미 응답이야",
                reply_to="1700000040.000100",
                metadata={"thread_id": "1700000040.000001"},
            )
        )

        self.assertTrue(result.success)
        rows = self.outbox_rows()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(result.message_id, str(row["id"]))
        self.assertEqual(row["channel_id"], "C0B69KP8G2J")
        self.assertEqual(row["thread_ts"], "1700000040.000001")
        self.assertEqual(row["text"], "차미 응답이야")
        self.assertEqual(row["created_by"], "queue:chami")
        self.assertEqual(row["status"], "pending")

    def test_send_falls_back_to_reply_to_for_thread(self):
        adapter = self.make_adapter()
        adapter._repo = self.make_repo()

        result = asyncio.run(
            adapter.send("queue:C0B69KP8G2J", "스레드 폴백", reply_to="1700000041.000100")
        )

        self.assertTrue(result.success)
        row = self.outbox_rows()[0]
        self.assertEqual(row["thread_ts"], "1700000041.000100")


class TestConnectLoopAndDisconnect(QueueAdapterTestBase):
    """T8: 실제 connect()로 폴링 루프가 돌고, disconnect()로 태스크가 종료된다."""

    def test_connect_polls_and_disconnect_stops_task(self):
        repo = self.make_repo()
        repo.insert_inbox(
            slack_event_ts="1700000050.000100",
            channel_id="C0B69KP8G2J",
            thread_ts="1700000050.000100",
            slack_user_id="U0CHAD",
            text="라이브 루프 턴",
            target="chami",
        )

        adapter = self.make_adapter()
        captured = []
        adapter.handle_message = self.completing_handle(adapter, captured)

        async def scenario():
            ok = await adapter.connect()
            self.assertTrue(ok)
            self.assertTrue(adapter._running)
            task = adapter._poll_task
            self.assertIsNotNone(task)

            for _ in range(100):  # 최대 ~5초 대기
                if captured:
                    break
                await asyncio.sleep(0.05)

            await adapter.disconnect()
            return task

        task = asyncio.run(scenario())

        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].text, "라이브 루프 턴")
        self.assertEqual(self.inbox_row("1700000050.000100")["status"], "done")
        self.assertFalse(adapter._running)
        self.assertIsNone(adapter._poll_task)
        self.assertTrue(task.done())


class TestReclaimStaleClaimed(QueueAdapterTestBase):
    """T9: 죽은 워커가 남긴 stale claimed row를 reclaim이 pending으로 복구한다."""

    def _backdate_claimed(self, slack_event_ts):
        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                "UPDATE slack_inbox SET claimed_at = '2000-01-01T00:00:00+00:00'"
                " WHERE slack_event_ts = ?",
                (slack_event_ts,),
            )
            con.commit()
        finally:
            con.close()

    def test_stale_claimed_row_recovered_and_reprocessable(self):
        repo = self.make_repo()
        repo.insert_inbox(
            slack_event_ts="1700000060.000100",
            channel_id="C0B69KP8G2J",
            thread_ts="1700000060.000100",
            slack_user_id="U0CHAD",
            text="고아가 될 턴",
            target="chami",
        )
        # 죽은 워커의 claim을 흉내: 음수 TTL -> 세션 락도 즉시 만료 상태.
        turn = repo.claim_session_turn(
            target="chami", worker="dead-worker", ttl_seconds=-10
        )
        self.assertIsNotNone(turn)
        self.assertEqual(self.inbox_row("1700000060.000100")["status"], "claimed")
        self._backdate_claimed("1700000060.000100")

        adapter = self.make_adapter()
        adapter._repo = repo

        asyncio.run(adapter._maybe_reclaim())
        self.assertEqual(self.inbox_row("1700000060.000100")["status"], "pending")

        # 복구된 row는 정상적으로 재처리된다.
        captured = []
        adapter.handle_message = self.completing_handle(adapter, captured)
        self.assertTrue(asyncio.run(adapter._poll_once()))
        self.assertEqual(len(captured), 1)
        self.assertEqual(self.inbox_row("1700000060.000100")["status"], "done")

    def test_fresh_claimed_row_not_reclaimed(self):
        repo = self.make_repo()
        repo.insert_inbox(
            slack_event_ts="1700000061.000100",
            channel_id="C0B69KP8G2J",
            thread_ts="1700000061.000100",
            slack_user_id="U0CHAD",
            text="처리 중인 턴",
            target="chami",
        )
        turn = repo.claim_session_turn(
            target="chami", worker="alive-worker", ttl_seconds=600
        )
        self.assertIsNotNone(turn)

        adapter = self.make_adapter()
        adapter._repo = repo
        asyncio.run(adapter._maybe_reclaim())
        # 방금 claim된(TTL 이내) row는 건드리지 않는다.
        self.assertEqual(self.inbox_row("1700000061.000100")["status"], "claimed")


class TestEndToEndCoreDispatch(QueueAdapterTestBase):
    """T10: 진짜 base.handle_message 파이프라인 통과 — 스폰된 백그라운드 처리에서
    핸들러 실행 -> send(outbox INSERT) -> on_processing_complete 훅 -> done."""

    def outbox_rows(self):
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        try:
            return con.execute(
                "SELECT channel_id, thread_ts, text FROM slack_outbox ORDER BY id"
            ).fetchall()
        finally:
            con.close()

    def test_real_pipeline_marks_done_and_writes_outbox(self):
        repo = self.make_repo()
        repo.insert_inbox(
            slack_event_ts="1700000070.000100",
            channel_id="C0B69KP8G2J",
            thread_ts="1700000070.000001",
            slack_user_id="U0CHAD",
            text="핑",
            target="chami",
        )

        adapter = self.make_adapter()
        adapter._repo = repo

        async def handler(event):
            return "퐁 응답이야"

        adapter.set_message_handler(handler)

        async def scenario():
            self.assertTrue(await adapter._poll_once())
            # handle_message는 즉시 리턴 -> 백그라운드 완료를 폴링으로 대기.
            for _ in range(200):  # 최대 ~10초
                if self.inbox_row("1700000070.000100")["status"] == "done":
                    break
                await asyncio.sleep(0.05)
            # 백그라운드 태스크 잔여 정리 시간을 살짝 준다.
            await asyncio.sleep(0.2)

        asyncio.run(scenario())

        self.assertEqual(self.inbox_row("1700000070.000100")["status"], "done")
        rows = self.outbox_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["channel_id"], "C0B69KP8G2J")
        self.assertEqual(rows[0]["thread_ts"], "1700000070.000001")
        self.assertIn("퐁 응답이야", rows[0]["text"])
        self.assertNotIn("1700000070.000100", adapter._inflight)


# ─────────────────────────────────────────────────────────────────────────────
# M1: 아웃바운드 handoff — send(target=상대에이전트) → 상대 slack_inbox INSERT.
#   접두 규약: chat_id 'queue:<채널>' → outbox(자동 응답), 그 외 → handoff.
# ─────────────────────────────────────────────────────────────────────────────


class M1TestBase(QueueAdapterTestBase):
    """M1 공용 헬퍼(slack_inbox/slack_outbox 직접 조회)."""

    def inbox_by_target(self, target):
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        try:
            return con.execute(
                "SELECT slack_event_ts, channel_id, thread_ts, slack_user_id,"
                " text, target, status FROM slack_inbox WHERE target = ? ORDER BY id",
                (target,),
            ).fetchall()
        finally:
            con.close()

    def inbox_count(self):
        con = sqlite3.connect(self.db_path)
        try:
            return con.execute("SELECT COUNT(*) FROM slack_inbox").fetchone()[0]
        finally:
            con.close()

    def outbox_rows(self):
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        try:
            return con.execute(
                "SELECT id, channel_id, thread_ts, text, created_by, status"
                " FROM slack_outbox ORDER BY id"
            ).fetchall()
        finally:
            con.close()


class TestM1HandoffSend(M1TestBase):
    """M1-T1: send(chat_id=chadol) → slack_inbox에 target=chadol row(outbox 아님)."""

    def test_handoff_inserts_inbox_row_for_target(self):
        adapter = self.make_adapter()  # QUEUE_AGENT=chami
        adapter._repo = self.make_repo()

        result = asyncio.run(adapter.send("chadol", "차돌아 이거 좀 봐줘"))

        self.assertTrue(result.success)
        rows = self.inbox_by_target("chadol")
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["target"], "chadol")
        self.assertEqual(row["slack_user_id"], "chami")
        self.assertEqual(row["text"], "차돌아 이거 좀 봐줘")
        self.assertEqual(row["status"], "pending")
        self.assertTrue(row["channel_id"].startswith("queue:handoff:chami->chadol"))
        self.assertEqual(result.message_id, row["slack_event_ts"])
        # 자동 응답 경로(outbox)로는 새지 않는다.
        self.assertEqual(len(self.outbox_rows()), 0)


class TestM1ReplyStaysOutbox(M1TestBase):
    """M1-T2: send(chat_id=queue:m0) → 기존대로 outbox INSERT(handoff 아님)."""

    def test_prefixed_channel_routes_to_outbox(self):
        adapter = self.make_adapter()
        adapter._repo = self.make_repo()

        result = asyncio.run(
            adapter.send("queue:m0", "자동 응답이야", reply_to="1700000100.000100")
        )

        self.assertTrue(result.success)
        # handoff inbox row는 생기지 않는다(inbox 총 0건).
        self.assertEqual(self.inbox_count(), 0)
        rows = self.outbox_rows()
        self.assertEqual(len(rows), 1)
        # 접두를 벗긴 raw 채널로 발송(센더 allowed_channel_ids 게이트 통과).
        self.assertEqual(rows[0]["channel_id"], "m0")
        self.assertEqual(rows[0]["text"], "자동 응답이야")


class TestM1HandoffThread(M1TestBase):
    """M1-T3: handoff 시 metadata thread_id → insert된 row의 thread_ts로 전달."""

    def test_metadata_thread_id_becomes_inbox_thread_ts(self):
        adapter = self.make_adapter()
        adapter._repo = self.make_repo()

        result = asyncio.run(
            adapter.send("chadol", "스레드 유지 인계", metadata={"thread_id": "T-CTX-123"})
        )

        self.assertTrue(result.success)
        row = self.inbox_by_target("chadol")[0]
        self.assertEqual(row["thread_ts"], "T-CTX-123")


class TestM1StandaloneSend(M1TestBase):
    """M1-T4: _standalone_send(게이트웨이 없이) → tmp DB에 insert_inbox target row."""

    class _Cfg:
        def __init__(self, extra):
            self.extra = extra

    def test_standalone_send_inserts_handoff_row(self):
        from plugins.platforms.queue.adapter import _standalone_send

        # 스키마 보장 + 이후 조회를 위해 repo를 미리 연다.
        self.make_repo()
        cfg = self._Cfg(
            {"db_path": self.db_path, "agent": "chami", "repo_root": SLACK_AGENT_ROOT}
        )

        result = asyncio.run(
            _standalone_send(cfg, "chadol", "스탠드얼론 인계", thread_id="TT-9")
        )

        self.assertTrue(result.get("success"))
        rows = self.inbox_by_target("chadol")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["slack_user_id"], "chami")
        self.assertEqual(rows[0]["text"], "스탠드얼론 인계")
        self.assertEqual(rows[0]["thread_ts"], "TT-9")
        self.assertEqual(str(result.get("message_id")), rows[0]["slack_event_ts"])

    def test_standalone_send_prefixed_channel_goes_to_outbox(self):
        from plugins.platforms.queue.adapter import _standalone_send

        self.make_repo()
        cfg = self._Cfg(
            {"db_path": self.db_path, "agent": "chami", "repo_root": SLACK_AGENT_ROOT}
        )

        result = asyncio.run(_standalone_send(cfg, "queue:m0", "스탠드얼론 응답"))

        self.assertTrue(result.get("success"))
        self.assertEqual(self.inbox_count(), 0)
        rows = self.outbox_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["channel_id"], "m0")


class TestM1DefensiveTargets(M1TestBase):
    """M1-T5 (negative): 빈 target / 자기 자신 target → 삽입 없이 에러 SendResult."""

    def test_empty_target_errors_without_insert(self):
        adapter = self.make_adapter()
        adapter._repo = self.make_repo()

        result = asyncio.run(adapter.send("", "아무 데도 못 감"))

        self.assertFalse(result.success)
        self.assertEqual(self.inbox_count(), 0)
        self.assertEqual(len(self.outbox_rows()), 0)

    def test_self_target_errors_without_insert(self):
        adapter = self.make_adapter()  # agent=chami
        adapter._repo = self.make_repo()

        result = asyncio.run(adapter.send("chami", "자기 자신에게 인계 금지"))

        self.assertFalse(result.success)
        self.assertIn("self", result.error)
        self.assertEqual(self.inbox_count(), 0)
        self.assertEqual(len(self.outbox_rows()), 0)

    def test_raw_slack_channel_id_target_rejected_without_insert(self):
        # 에이전트가 raw 슬랙 채널ID를 큐 handoff target으로 넘기면(접두 'queue:'
        # 없이), 아무도 처리 못 하는 죽은 inbox row가 생기고 발신자에겐 success로
        # 보여 실패가 은폐된다 → 삽입 없이 명시적 에러여야 한다.
        adapter = self.make_adapter()  # agent=chami
        adapter._repo = self.make_repo()

        result = asyncio.run(adapter.send("C0B69KP8G2J", "raw 채널ID는 handoff 대상 아님"))

        self.assertFalse(result.success)
        self.assertIn("raw slack channel id", result.error)
        # 죽은 handoff row도, outbox row도 만들지 않는다.
        self.assertEqual(self.inbox_count(), 0)
        self.assertEqual(len(self.outbox_rows()), 0)

    def test_prefixed_channel_still_routes_to_outbox_not_rejected(self):
        # raw 채널ID 거부가 정당한 자동응답 경로('queue:<채널>')까지 막지 않는지
        # 회귀 방어 — 접두가 붙은 채널은 그대로 outbox로 가야 한다.
        adapter = self.make_adapter()
        adapter._repo = self.make_repo()

        result = asyncio.run(adapter.send("queue:C0B69KP8G2J", "정상 자동응답"))

        self.assertTrue(result.success)
        self.assertEqual(self.inbox_count(), 0)
        rows = self.outbox_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["channel_id"], "C0B69KP8G2J")


class TestM1DuplicateHandoff(M1TestBase):
    """M1-T6: handoff insert_inbox 중복 event_ts → success=False."""

    def test_duplicate_event_ts_reports_failure(self):
        import plugins.platforms.queue.adapter as qa

        adapter = self.make_adapter()
        adapter._repo = self.make_repo()

        fixed = uuid.UUID("00000000-0000-0000-0000-000000000abc")
        with patch.object(qa.uuid, "uuid4", return_value=fixed):
            r1 = asyncio.run(
                adapter.send("chadol", "첫 인계", metadata={"thread_id": "TT"})
            )
            r2 = asyncio.run(
                adapter.send("chadol", "둘째 인계", metadata={"thread_id": "TT"})
            )

        self.assertTrue(r1.success)
        self.assertFalse(r2.success)
        self.assertIn("duplicate", r2.error)
        # 중복은 삽입되지 않는다(원본 1건만).
        self.assertEqual(len(self.inbox_by_target("chadol")), 1)


class TestM1AutoReplyPipelineStaysOutbox(M1TestBase):
    """M1-T7 (통합): 실 base handle_message 파이프라인 — 자동 응답이 handoff가
    아니라 outbox로 가는지(M0 의미 회귀). 인바운드 채널은 'queue:m0'."""

    def test_real_pipeline_autoreply_writes_outbox_not_handoff(self):
        repo = self.make_repo()
        repo.insert_inbox(
            slack_event_ts="1700000090.000100",
            channel_id="queue:m0",
            thread_ts="1700000090.000001",
            slack_user_id="U0CHAD",
            text="핑",
            target="chami",
        )

        adapter = self.make_adapter()
        adapter._repo = repo

        async def handler(event):
            return "퐁 응답이야"

        adapter.set_message_handler(handler)

        async def scenario():
            self.assertTrue(await adapter._poll_once())
            for _ in range(200):  # 최대 ~10초
                if self.inbox_row("1700000090.000100")["status"] == "done":
                    break
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.2)

        asyncio.run(scenario())

        self.assertEqual(self.inbox_row("1700000090.000100")["status"], "done")
        # 자동 응답은 outbox로(접두 제거된 raw 채널 'm0').
        rows = self.outbox_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["channel_id"], "m0")
        self.assertIn("퐁 응답이야", rows[0]["text"])
        # 새 handoff inbox row 없음 — 원본 인바운드 1건만 남는다.
        self.assertEqual(self.inbox_count(), 1)
