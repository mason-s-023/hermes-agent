"""queue_handoff agent-callable 도구 테스트 (TDD).

큐 내부 에이전트 간 handoff 전용 도구. send_message처럼 외부 플랫폼으로
발신하는 게 아니라, slack_agent 로컬 SQLite 큐(bridge.local_repo.SQLiteQueueRepo)의
slack_inbox에 target=<상대 에이전트>, slack_user_id=<자기 에이전트>로 INSERT 해서
상대 워커가 claim 하게 만든다.

tmp SQLite + env 픽스처. sys.path 조작(QUEUE_REPO_ROOT)은 tool 본체가 수행한다.

커버:
T1 정상 handoff -> slack_inbox에 target/slack_user_id/text row 생성 + success 반환
T2 check_fn: QUEUE_AGENT/QUEUE_REPO_ROOT + QUEUE_DB_PATH 또는 QUEUE_ENDPOINT 필요
T3 (negative) to=자기자신 / to="" / to=raw 슬랙채널ID -> error + insert 없음
T4 (registry) import 시 registry에 name=queue_handoff 자동 등록(toolset/check_fn/is_async)
T5 반환 형식이 도구 반환 규약(JSON str: success/target/message_id 또는 error)과 일치
"""

import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

# slack_agent 레포 루트(bridge 패키지 제공). 라이브 기본값 = 스미스 로컬 체크아웃.
SLACK_AGENT_ROOT = os.environ.get(
    "QUEUE_TEST_REPO_ROOT", "/Users/charde023/workspace/slack_agent"
)
_HAS_SLACK_AGENT = (
    Path(SLACK_AGENT_ROOT, "bridge", "local_repo.py").is_file()
    and Path(SLACK_AGENT_ROOT, "bridge", "agent_repo.py").is_file()
)

if _HAS_SLACK_AGENT and SLACK_AGENT_ROOT not in sys.path:
    # append — insert(0)은 slack_agent 루트의 config.py(시크릿)가 전역 최우선
    # import 되는 구조라 금지(어댑터/도구 본체와 동일 규칙).
    sys.path.append(SLACK_AGENT_ROOT)


@unittest.skipUnless(_HAS_SLACK_AGENT, f"slack_agent repo not found: {SLACK_AGENT_ROOT}")
class QueueHandoffToolTestBase(unittest.TestCase):
    """tmp SQLite + env 픽스처 공통 베이스."""

    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db_path = str(Path(tmpdir.name) / "queue-test.sqlite3")
        self.env = {
            "QUEUE_DB_PATH": self.db_path,
            "QUEUE_AGENT": "chami",
            "QUEUE_REPO_ROOT": SLACK_AGENT_ROOT,
        }
        env_guard = patch.dict(os.environ, self.env, clear=False)
        env_guard.start()
        self.addCleanup(env_guard.stop)
        # 주변 셸/게이트웨이 세션 env가 새어 위양성을 내지 않게 정리.
        for k in ("HERMES_SESSION_THREAD_ID", "HERMES_SESSION_CHAT_ID", "QUEUE_ENDPOINT", "QUEUE_TOKEN"):
            os.environ.pop(k, None)

    def make_repo(self):
        from bridge.local_repo import SQLiteQueueRepo

        # 생성 시 _init_schema()로 slack_inbox/outbox 테이블 자동 생성.
        return SQLiteQueueRepo(self.db_path)

    def call_tool(self, **args):
        from tools.queue_handoff_tool import queue_handoff_tool

        result = asyncio.run(queue_handoff_tool(args))
        return json.loads(result)

    def inbox_rows(self):
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        try:
            return [
                dict(r)
                for r in con.execute(
                    "SELECT slack_user_id, target, text, status FROM slack_inbox"
                ).fetchall()
            ]
        finally:
            con.close()


class TestHandoffInsert(QueueHandoffToolTestBase):
    """T1: 정상 handoff → slack_inbox row 생성 + success 반환."""

    def test_handoff_inserts_inbox_row(self):
        self.make_repo()  # 스키마 선생성
        result = self.call_tool(to="chadol", message="배포 좀 봐줘")

        self.assertTrue(result.get("success"), result)
        self.assertEqual(result.get("target"), "chadol")
        self.assertTrue(result.get("message_id"))

        rows = self.inbox_rows()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["target"], "chadol")
        self.assertEqual(row["slack_user_id"], "chami")  # 발신 = 자기 에이전트
        self.assertEqual(row["text"], "배포 좀 봐줘")
        self.assertEqual(row["status"], "pending")

    def test_handoff_uses_session_thread_when_present(self):
        self.make_repo()
        with patch.dict(os.environ, {"HERMES_SESSION_THREAD_ID": "1720000000.111"}):
            result = self.call_tool(to="chadol", message="스레드 이어가기")
        self.assertTrue(result.get("success"), result)
        con = sqlite3.connect(self.db_path)
        try:
            thread_ts = con.execute(
                "SELECT thread_ts FROM slack_inbox WHERE target='chadol'"
            ).fetchone()[0]
        finally:
            con.close()
        self.assertEqual(thread_ts, "1720000000.111")

    def test_chat_id_not_used_as_thread_ts(self):
        # THREAD_ID 없이 CHAT_ID(채널 식별자)만 있을 때, 채널ID를 thread_ts로
        # 쓰면 안 된다(수신측 세션 오분류) — 합성 qh- 스레드로 새로 시작해야 한다.
        self.make_repo()
        with patch.dict(os.environ, {"HERMES_SESSION_CHAT_ID": "C0B69KP8G2J"}):
            result = self.call_tool(to="chadol", message="채널ID 폴백 금지")
        self.assertTrue(result.get("success"), result)
        self.assertNotEqual(result.get("thread_ts"), "C0B69KP8G2J")
        self.assertTrue(str(result.get("thread_ts")).startswith("qh-"), result)


class TestCheckFn(QueueHandoffToolTestBase):
    """T2: check_fn — agent/root + db_path 또는 endpoint가 필요."""

    def test_all_env_present_true(self):
        from tools.queue_handoff_tool import check_queue_handoff

        self.assertTrue(check_queue_handoff())

    def test_missing_agent_or_repo_root_false(self):
        from tools.queue_handoff_tool import check_queue_handoff

        for key in ("QUEUE_AGENT", "QUEUE_REPO_ROOT"):
            with self.subTest(missing=key):
                with patch.dict(os.environ, {}, clear=False):
                    os.environ.pop(key, None)
                    self.assertFalse(check_queue_handoff())

    def test_missing_db_path_false_without_endpoint(self):
        from tools.queue_handoff_tool import check_queue_handoff

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("QUEUE_DB_PATH", None)
            os.environ.pop("QUEUE_ENDPOINT", None)
            self.assertFalse(check_queue_handoff())

    def test_endpoint_allows_missing_db_path(self):
        from tools.queue_handoff_tool import check_queue_handoff

        with patch.dict(os.environ, {"QUEUE_ENDPOINT": "http://127.0.0.1:8770"}, clear=False):
            os.environ.pop("QUEUE_DB_PATH", None)
            self.assertTrue(check_queue_handoff())


class TestDefenses(QueueHandoffToolTestBase):
    """T3: 방어 — 자기자신/빈값/raw 슬랙채널ID는 error + insert 없음."""

    def _assert_error_no_insert(self, **args):
        self.make_repo()
        result = self.call_tool(**args)
        self.assertIn("error", result)
        self.assertNotIn("success", result)
        self.assertEqual(self.inbox_rows(), [])

    def test_handoff_to_self_rejected(self):
        self._assert_error_no_insert(to="chami", message="자기 자신")

    def test_empty_target_rejected(self):
        self._assert_error_no_insert(to="   ", message="빈 타겟")

    def test_raw_slack_channel_id_rejected(self):
        self._assert_error_no_insert(to="C0B69KP8G2J", message="채널ID 오입력")

    def test_case_variant_of_self_rejected(self):
        # 'Chami'는 자기 자신(QUEUE_AGENT=chami)의 대소문자 변형 — self 가드를
        # 우회하면 아무도 claim 못 하는 죽은 row가 된다.
        self._assert_error_no_insert(to="Chami", message="대문자 self")

    def test_unknown_agent_key_rejected(self):
        # 오타·환각 키는 어떤 워커도 claim 못 하므로 삽입 없이 거부.
        self._assert_error_no_insert(to="chadl", message="오타 타겟")
        self._assert_error_no_insert(to="claude", message="로스터 밖 키")

    def test_oversized_message_rejected(self):
        self._assert_error_no_insert(to="chadol", message="x" * 40_001)

    def test_uppercase_valid_agent_normalized_and_delivered(self):
        # 'Chadol'은 유효 에이전트의 대소문자 변형 — 소문자 canonical로 정규화해
        # target='chadol'로 배달되어야 한다(수신 워커의 QUEUE_AGENT와 정확일치).
        self.make_repo()
        result = self.call_tool(to="Chadol", message="대소문자 정규화")
        self.assertTrue(result.get("success"), result)
        self.assertEqual(result.get("target"), "chadol")
        rows = self.inbox_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["target"], "chadol")

    def test_known_agents_env_override(self):
        # QUEUE_KNOWN_AGENTS로 로스터를 확장하면 그 키로 handoff 가능.
        self.make_repo()
        with patch.dict(os.environ, {"QUEUE_KNOWN_AGENTS": "kc,zed"}):
            result = self.call_tool(to="kc", message="확장 로스터")
        self.assertTrue(result.get("success"), result)
        self.assertEqual(self.inbox_rows()[0]["target"], "kc")

    def test_missing_agent_identity_rejected(self):
        # QUEUE_AGENT 없으면 발신자 정체성이 없어 handoff 불가.
        self.make_repo()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("QUEUE_AGENT", None)
            result = self.call_tool(to="chadol", message="정체성 없음")
        self.assertIn("error", result)
        self.assertEqual(self.inbox_rows(), [])


class TestRegistryRegistration(QueueHandoffToolTestBase):
    """T4: import 시 registry 자동 등록 확인."""

    def test_registered_in_registry(self):
        import tools.queue_handoff_tool  # noqa: F401  (등록 부작용)
        from tools.registry import registry

        entry = registry.get_entry("queue_handoff")
        self.assertIsNotNone(entry, "queue_handoff 도구가 registry에 없음")
        self.assertEqual(entry.name, "queue_handoff")
        self.assertEqual(entry.toolset, "queue")  # hermes-queue 번들 자동 편입 조건
        self.assertTrue(entry.is_async)
        self.assertIsNotNone(entry.check_fn)

    def test_exposed_via_get_definitions_when_env_present(self):
        import tools.queue_handoff_tool  # noqa: F401
        from tools.registry import registry

        # check_fn 30s TTL 캐시 회피 위해 이름으로 직접 조회.
        defs = registry.get_definitions({"queue_handoff"})
        names = [d["function"]["name"] for d in defs]
        self.assertIn("queue_handoff", names)

    def test_exposed_in_hermes_queue_toolset(self):
        # 실제 세션-노출 경로: resolve_toolset("hermes-queue")의 플러그인-플랫폼
        # 자동생성 분기가 toolset=="queue" 도구를 끌어와야 큐 세션이 이 도구를
        # 본다. registry 등록/check_fn만으로는 이 경로를 증명하지 못한다 —
        # toolset명 변경·자동생성 분기 회귀를 이 테스트가 잡는다.
        import tools.queue_handoff_tool  # noqa: F401
        from toolsets import resolve_toolset
        from gateway.platform_registry import platform_registry

        with patch.object(
            platform_registry, "is_registered", side_effect=lambda p: p == "queue"
        ):
            resolved = resolve_toolset("hermes-queue")
        self.assertIn("queue_handoff", resolved)


class TestReturnContract(QueueHandoffToolTestBase):
    """T5: 반환 규약 — 성공/실패 모두 JSON str."""

    def test_returns_json_string(self):
        from tools.queue_handoff_tool import queue_handoff_tool

        self.make_repo()
        raw = asyncio.run(queue_handoff_tool({"to": "chadol", "message": "문자열 반환"}))
        self.assertIsInstance(raw, str)
        parsed = json.loads(raw)
        self.assertEqual(set(parsed) >= {"success", "target", "message_id"}, True)

    def test_error_returns_json_string(self):
        from tools.queue_handoff_tool import queue_handoff_tool

        self.make_repo()
        raw = asyncio.run(queue_handoff_tool({"to": "chami", "message": "self"}))
        self.assertIsInstance(raw, str)
        self.assertIn("error", json.loads(raw))


if __name__ == "__main__":
    unittest.main()
