# -*- coding: utf-8 -*-
"""
xtdata 入库脚本入口测试。

Responsibilities:
    - 校验三个一键脚本是否调用正确的 profile。
    - 校验参数化主入口是否正确解析模式与覆盖参数。
"""
from __future__ import annotations

import unittest
from unittest import mock

import scripts.xtdata_ingest as ingest_cli
import scripts.xtdata_ingest_backfill as ingest_backfill
import scripts.xtdata_ingest_full as ingest_full
import scripts.xtdata_ingest_recent as ingest_recent


class TestIngestOneClickScripts(unittest.TestCase):
    """
    一键脚本调用关系测试。
    """

    def test_full_script_calls_full_download(self):
        """
        校验 full 一键脚本调用 full-download。

        Returns:
            None
        """
        with mock.patch.object(ingest_full, "run_profile") as mocked:
            ingest_full.main()
        mocked.assert_called_once_with("full-download")

    def test_backfill_script_calls_full_backfill(self):
        """
        校验 backfill 一键脚本调用 full-backfill。

        Returns:
            None
        """
        with mock.patch.object(ingest_backfill, "run_profile") as mocked:
            ingest_backfill.main()
        mocked.assert_called_once_with("full-backfill")

    def test_recent_script_calls_recent_backfill(self):
        """
        校验 recent 一键脚本调用 recent-backfill。

        Returns:
            None
        """
        with mock.patch.object(ingest_recent, "run_profile") as mocked:
            ingest_recent.main()
        mocked.assert_called_once_with("recent-backfill")


class TestIngestCli(unittest.TestCase):
    """
    参数化入口解析测试。
    """

    def test_cli_calls_run_profile_with_overrides(self):
        """
        校验 CLI 会把模式与覆盖参数传给 run_profile。

        Returns:
            None
        """
        argv = [
            "recent-backfill",
            "--symbols",
            "AAA.SH,BBB.SH",
            "--cycles",
            "1m,1d",
            "--root",
            "D:/data",
            "--lookback",
            "5",
            "--no-auto-start",
            "--merge",
        ]
        with mock.patch.object(ingest_cli, "run_profile", return_value={"ok": True}) as mocked:
            rc = ingest_cli.main(argv)

        self.assertEqual(rc, 0)
        self.assertEqual(mocked.call_count, 1)
        mode = mocked.call_args.args[0]
        kwargs = mocked.call_args.kwargs
        self.assertEqual(mode, "recent-backfill")
        self.assertEqual(kwargs["symbols"], ("AAA.SH", "BBB.SH"))
        self.assertEqual(kwargs["cycles"], ("1m", "1d"))
        self.assertEqual(kwargs["root"], "D:/data")
        self.assertEqual(kwargs["lookback"], 5)
        self.assertFalse(kwargs["auto_start"])
        self.assertTrue(kwargs["merge"])

    def test_cli_failure_returns_2(self):
        """
        校验 run_profile 抛异常时 CLI 返回 2。

        Returns:
            None
        """
        with mock.patch.object(ingest_cli, "run_profile", side_effect=RuntimeError("boom")):
            rc = ingest_cli.main(["full-download"])
        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
