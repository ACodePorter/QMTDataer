# -*- coding: utf-8 -*-
"""
xtdata 入库执行器，统一全量下载、全量补齐与近期补齐三种模式。

Responsibilities:
    - 维护三种入库模式的默认配置，提供可覆盖参数的 profile 构建能力。
    - 统一执行 download + fetch + merge/save 流程，输出结构化汇总结果。
    - 提供 auto_start + lookback 的近期增量回溯能力。
    - 提供运行前探针，提前识别 xtdata 不可用或返回空数据的场景。

Data Contract:
    - 本模块对输入数据结构不做统一约束，具体约束见各函数 docstring。
    - 对外暴露的 profile 必须包含 symbols、cycles、root、market、specific 等参数。

Internal Dependencies:
    - core.xtdata_source: 提供 XtdataSource 适配器。
    - core.ingestor: 提供 MarketDataIngestor 流程编排。
    - core.storage_simple: 提供本地目录结构与文件命名规则。

External Systems:
    - xtquant.xtdata（或兼容的 xtdata 模块）
    - 本地文件系统
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta, timezone
from pathlib import Path
from typing import Any, Optional
import logging

import pandas as pd

from core.ingestor import MarketDataIngestor
from core.storage_simple import FinancialDataStorage
from core.xtdata_source import XtdataSource

CN_TZ = timezone(timedelta(hours=8))
DEFAULT_ROOT = "D:/Work/Quant/financial_database"
DEFAULT_MARKET = "SS_stock_data"
DEFAULT_SPECIFIC = "original"
DEFAULT_CYCLES = ("1d", "1m")
DEFAULT_SYMBOLS = (
    "159915.SZ",
    "518880.SH",
    "513880.SH",
    "513100.SH",
    "513030.SH",
    "513080.SH",
    "513180.SH",
    "510300.SH",
    "511010.SH",
    "159980.SZ",
    "563300.SH",
    "563680.SH",
    "515790.SH",
    "513130.SH",
    "512480.SH",
    "510050.SH",
    "000001.SH",
    "563580.SH",
    "510880.SH",
    "159985.SZ",
    "513400.SH",
    "513520.SH",
    "513600.SH",
    "159119.SZ",
)
DEFAULT_SYMBOLS_RECENT = (
    "513880.SH",
    "518880.SH",
    "000001.SH",
)


@dataclass(frozen=True)
class IngestProfile:
    """
    入库模式配置对象。

    Attributes:
        name (str): 模式名称。
        symbols (tuple[str, ...]): 目标标的列表。
        cycles (tuple[str, ...]): 目标周期列表。
        root (str): 目标根目录。
        market (str): 市场目录名。
        specific (str): 子目录或合成标记。
        start (str): 起始时间，xtdata 格式。
        end (str): 结束时间，xtdata 格式。空字符串表示自动推导。
        skip_download (bool): 是否跳过 download_history_data。
        auto_start (bool): 是否基于本地已有文件自动回溯起点。
        lookback (int): auto_start 场景下回溯 bar 数。
        merge (bool): 是否采用 merge 模式写入文件。
    """

    name: str
    symbols: tuple[str, ...]
    cycles: tuple[str, ...]
    root: str = DEFAULT_ROOT
    market: str = DEFAULT_MARKET
    specific: str = DEFAULT_SPECIFIC
    start: str = "20000101"
    end: str = ""
    skip_download: bool = False
    auto_start: bool = False
    lookback: int = 2
    merge: bool = True


def _default_profiles() -> dict[str, IngestProfile]:
    """
    返回三种模式的默认 profile。

    Returns:
        dict[str, IngestProfile]: 键为模式名，值为对应默认配置。
    """
    return {
        "full-download": IngestProfile(
            name="full-download",
            symbols=DEFAULT_SYMBOLS,
            cycles=DEFAULT_CYCLES,
            skip_download=False,
            auto_start=False,
            lookback=0,
            merge=False,
        ),
        "full-backfill": IngestProfile(
            name="full-backfill",
            symbols=DEFAULT_SYMBOLS,
            cycles=DEFAULT_CYCLES,
            skip_download=False,
            auto_start=False,
            lookback=2,
            merge=True,
        ),
        "recent-backfill": IngestProfile(
            name="recent-backfill",
            symbols=DEFAULT_SYMBOLS_RECENT,
            cycles=DEFAULT_CYCLES,
            skip_download=False,
            auto_start=True,
            lookback=2,
            merge=True,
        ),
    }


def list_profile_names() -> tuple[str, ...]:
    """
    返回当前支持的模式名称列表。

    Returns:
        tuple[str, ...]: 可用模式名。
    """
    return tuple(_default_profiles().keys())


def build_profile(name: str, **overrides: Any) -> IngestProfile:
    """
    根据模式名构建 profile，并允许字段覆盖。

    Args:
        name (str): 模式名，必须来自 list_profile_names()。
        **overrides (Any): 要覆盖的 dataclass 字段。

    Returns:
        IngestProfile: 构建后的 profile 对象。
    """
    profiles = _default_profiles()
    if name not in profiles:
        raise ValueError(f"未知模式: {name}，可选: {list_profile_names()}")
    profile = profiles[name]
    if not overrides:
        return profile

    fields = set(profile.__dataclass_fields__.keys())
    safe_overrides = {k: v for k, v in overrides.items() if k in fields and v is not None}
    if "symbols" in safe_overrides:
        safe_overrides["symbols"] = tuple(safe_overrides["symbols"])
    if "cycles" in safe_overrides:
        safe_overrides["cycles"] = tuple(safe_overrides["cycles"])
    return replace(profile, **safe_overrides)


def _import_xtdata() -> Any:
    """
    动态导入 xtdata 模块。

    Returns:
        Any: 可调用的 xtdata 模块对象。
    """
    try:
        from xtquant import xtdata  # type: ignore
        return xtdata
    except Exception:
        try:
            import xtdata  # type: ignore
            return xtdata
        except Exception as exc:
            raise RuntimeError(
                "无法导入 xtdata，请确认已安装并在 MiniQMT 环境运行。"
            ) from exc


def _resolve_end_time(end_text: str) -> str:
    """
    解析结束时间。

    Args:
        end_text (str): 结束时间字符串。

    Returns:
        str: 非空结束时间，格式为 YYYYMMDD。
    """
    if end_text:
        return end_text
    end_dt = pd.Timestamp.now(tz=CN_TZ) + pd.Timedelta(days=1)
    return end_dt.strftime("%Y%m%d")


def _probe_xtdata(
    xtdata_mod: Any,
    symbol: str,
    cycle: str,
    start: str,
    end: str,
    logger: logging.Logger,
) -> None:
    """
    执行小范围探针，提前验证 xtdata 可用性。

    Args:
        xtdata_mod (Any): xtdata 模块对象。
        symbol (str): 用于探针的标的代码。
        cycle (str): 用于探针的周期。
        start (str): 探针起始时间。
        end (str): 探针结束时间。
        logger (logging.Logger): 日志对象。

    Returns:
        None
    """
    try:
        xtdata_mod.download_history_data(
            stock_code=symbol,
            period=cycle,
            start_time=start,
            end_time=end,
            incrementally=True,
        )
    except Exception as exc:
        logger.warning("xtdata 探针 download_history_data 失败，将继续尝试 get: %s", exc)

    try:
        data_dict = xtdata_mod.get_market_data_ex(
            stock_list=[symbol],
            period=cycle,
            start_time=start,
            end_time=end,
            count=-1,
            dividend_type="none",
            fill_data=False,
            field_list=[],
        )
    except Exception as exc:
        raise RuntimeError(f"xtdata 探针 get_market_data_ex 失败: {exc}") from exc

    if not isinstance(data_dict, dict) or not data_dict:
        raise RuntimeError("xtdata 探针返回空数据，可能未登录 MiniQMT 或无行情权限。")


def _infer_freq_timedelta(cycle: str) -> Optional[pd.Timedelta]:
    """
    根据周期字符串推断时间间隔。

    Args:
        cycle (str): 周期字符串，如 1m、1h、1d。

    Returns:
        Optional[pd.Timedelta]: 可识别时返回间隔，否则返回 None。
    """
    cycle_low = cycle.lower()
    if cycle_low.endswith("m"):
        try:
            minutes = int(cycle_low[:-1])
            return pd.to_timedelta(minutes, unit="m")
        except Exception:
            return pd.to_timedelta(1, unit="m")
    if cycle_low.endswith("h"):
        try:
            hours = int(cycle_low[:-1])
            return pd.to_timedelta(hours, unit="h")
        except Exception:
            return pd.to_timedelta(1, unit="h")
    if cycle_low.endswith("d"):
        return pd.to_timedelta(1, unit="d")
    return None


def _load_latest_start(
    storage: FinancialDataStorage,
    profile: IngestProfile,
    symbol: str,
    cycle: str,
) -> tuple[str, Optional[int]]:
    """
    基于本地已有文件计算 auto_start 场景的起始时间。

    Args:
        storage (FinancialDataStorage): 存储适配器。
        profile (IngestProfile): 当前运行配置。
        symbol (str): 标的代码。
        cycle (str): 周期字符串。

    Returns:
        tuple[str, Optional[int]]: 计算后的 start_time 与已有行数。
    """
    target_dir = storage._build_target_dir(profile.market, symbol, cycle, profile.specific)
    filename = storage._build_filename(profile.market, symbol, cycle, profile.specific, "csv")
    file_path = Path(target_dir) / filename
    if not file_path.exists():
        return profile.start, None

    try:
        existed_df = pd.read_csv(file_path)
    except Exception:
        return profile.start, None
    if existed_df.empty or "time" not in existed_df.columns:
        return profile.start, len(existed_df)

    times = pd.to_datetime(existed_df["time"], errors="coerce").dropna()
    if times.empty:
        return profile.start, len(existed_df)

    latest = times.max()
    freq = _infer_freq_timedelta(cycle)
    if freq is not None and profile.lookback > 0:
        latest = latest - freq * profile.lookback

    if cycle.lower().endswith(("m", "h")):
        start_text = latest.strftime("%Y%m%d%H%M%S")
    else:
        start_text = latest.strftime("%Y%m%d")
    return start_text, len(existed_df)


def _validate_output_file(path: Path) -> tuple[bool, str]:
    """
    校验输出文件是否满足最小质量要求。

    Args:
        path (Path): 目标文件路径。

    Returns:
        tuple[bool, str]: 校验通过返回 (True, "")，否则返回失败原因。
    """
    required_cols = {"time", "open", "high", "low", "close", "volume", "amount"}
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        return False, f"读取失败: {exc}"
    if df.empty:
        return False, "文件为空"
    if not required_cols.issubset(df.columns):
        return False, f"缺少列: {required_cols - set(df.columns)}"
    if not df["time"].is_monotonic_increasing:
        return False, "time 未按升序排列"
    return True, ""


def run_ingest(profile: IngestProfile, logger: Optional[logging.Logger] = None) -> dict[str, Any]:
    """
    按指定 profile 执行一次入库任务。

    Args:
        profile (IngestProfile): 运行配置对象。
        logger (Optional[logging.Logger]): 可选日志对象。

    Returns:
        dict[str, Any]: 包含 mode、ok_files、failed、total 的执行结果。
    """
    if not profile.symbols:
        raise ValueError("profile.symbols 不能为空。")
    if not profile.cycles:
        raise ValueError("profile.cycles 不能为空。")

    log = logger or logging.getLogger(__name__)
    xtdata_mod = _import_xtdata()
    end_use = _resolve_end_time(profile.end)
    _probe_xtdata(
        xtdata_mod=xtdata_mod,
        symbol=profile.symbols[0],
        cycle=profile.cycles[0],
        start=profile.start,
        end=end_use,
        logger=log,
    )

    storage = FinancialDataStorage(root_dir=profile.root)
    ingestor = MarketDataIngestor(storage)
    ok_files: list[str] = []
    failed: list[str] = []

    for cycle in profile.cycles:
        source = XtdataSource(xtdata=xtdata_mod, download=not profile.skip_download)
        for symbol in profile.symbols:
            start_use = profile.start
            existed_rows: Optional[int] = None
            if profile.auto_start:
                start_use, existed_rows = _load_latest_start(storage, profile, symbol, cycle)
            try:
                out_path = ingestor.ingest_symbol(
                    source=source,
                    market=profile.market,
                    symbol=symbol,
                    cycle=cycle,
                    specific=profile.specific,
                    start=start_use,
                    end=end_use,
                    file_type="csv",
                    time_column="time",
                    merge=profile.merge,
                )
                valid, reason = _validate_output_file(Path(out_path))
                if not valid:
                    failed.append(f"{symbol}-{cycle}: {reason}")
                    log.error("输出文件校验失败: %s %s -> %s", symbol, cycle, reason)
                    continue
                if existed_rows is not None:
                    try:
                        new_total = len(pd.read_csv(out_path))
                        added = max(0, new_total - existed_rows)
                        log.info("[INGEST] %s %s 完成，新增=%d 总计=%d", symbol, cycle, added, new_total)
                    except Exception:
                        log.info("[INGEST] %s %s 完成", symbol, cycle)
                else:
                    log.info("[INGEST] %s %s 完成", symbol, cycle)
                ok_files.append(out_path)
            except Exception as exc:
                failed.append(f"{symbol}-{cycle}: {exc}")
                log.exception("处理失败: %s %s", symbol, cycle)

    result = {
        "mode": profile.name,
        "ok_files": ok_files,
        "failed": failed,
        "total": len(ok_files) + len(failed),
    }
    if failed:
        raise RuntimeError(f"入库任务存在失败项: {failed}")
    return result


def run_profile(name: str, **overrides: Any) -> dict[str, Any]:
    """
    按模式名运行入库任务。

    Args:
        name (str): 模式名。
        **overrides (Any): 对 profile 字段的覆盖参数。

    Returns:
        dict[str, Any]: run_ingest 的执行结果。
    """
    profile = build_profile(name, **overrides)
    return run_ingest(profile)
