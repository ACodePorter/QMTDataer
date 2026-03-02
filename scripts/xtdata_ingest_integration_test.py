# -*- coding: utf-8 -*-
"""
xtdata 入库兼容入口脚本。

Responsibilities:
    - 保留历史脚本名，避免本地习惯调用立即失效。
    - 默认执行 full-backfill 模式，并允许指定其他模式。
    - 复用 core.ingest_runner 统一逻辑，不再维护重复实现。

Data Contract:
    - mode 必须来自 core.ingest_runner.list_profile_names()。

Internal Dependencies:
    - core.ingest_runner

External Systems:
    - xtquant.xtdata（或兼容 xtdata）
    - 本地文件系统
"""
from __future__ import annotations

import argparse
from typing import Optional

from core.ingest_runner import list_profile_names, run_profile


def main(argv: Optional[list[str]] = None) -> None:
    """
    执行兼容入口逻辑。

    Args:
        argv (Optional[list[str]]): 可选参数列表。

    Returns:
        None
    """
    parser = argparse.ArgumentParser(description="xtdata 入库兼容入口")
    parser.add_argument(
        "--mode",
        default="full-backfill",
        choices=list_profile_names(),
        help="运行模式，默认 full-backfill",
    )
    args = parser.parse_args(argv)
    run_profile(args.mode)


if __name__ == "__main__":
    main()
