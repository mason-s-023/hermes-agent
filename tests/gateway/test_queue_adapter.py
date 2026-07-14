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
        self.assertEqual(event.source.chat_id, "C0B69KP8G2J")
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

        result = asyncio.run(
            adapter.send(
                "C0B69KP8G2J",
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
            adapter.send("C0B69KP8G2J", "스레드 폴백", reply_to="1700000041.000100")
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
