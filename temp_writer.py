from pathlib import Path
content = """# -*- coding: utf-8 -*-
"""ű xtdata ȡʷ K ߲Ϊ CSV ļ

ʹ÷
    1. ȷǰ import xtquant.xtdata MiniQMT Ѿ¼
    2. ޸Ľűĳġڡʱ䴰ڡ·ȣ
    3. ֱУpython scripts/archive/dump_xtdata_csv.py
       ִɺ󣬻ѲѯĿд SAVE_PATH ָ CSV ļ

ֶ˵
    CODES      б ['510050.SH', '159915.SZ']
    PERIOD     ڣ 1m/5m/15m/30m/60m/1dȡ xtdata ֧
    START/END  ʱ䴰ڣ֧ YYYYMMDD  YYYYMMDDHHMMSS
    COUNT      Ʒ-1 ʾƣ
    SAVE_PATH   CSV ·
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

try:
    from xtquant import xtdata  # type: ignore
except Exception as exc:  # pragma: no cover
    raise RuntimeError(f"޷ xtquant.xtdata{exc}")

try:
    import pandas as pd
except Exception as exc:  # pragma: no cover
    raise RuntimeError(f"޷ pandas{exc}")

# ------------------  ------------------
CODES: List[str] = ["510050.SH", "159915.SZ"]
PERIOD: str = "1m"
START: str = "20240901093000"
END: str = "20240901100000"
COUNT: int = -1
SAVE_PATH: Path = Path("dump_xtdata.csv")
# ------------------  ------------------


def _normalize_dataframe(data_dict: Dict[str, Any]) -> pd.DataFrame:
    """ xtdata ص {field: DataFrame} ṹתΪʽ DataFrame"""
    time_df = data_dict.get("time")
    if time_df is None:
        raise ValueError("xtdata ؽȱ 'time' ֶ")

    frames = {"time": time_df.T}
    for field, df in data_dict.items():
        if field == "time":
            continue
        frames[field] = df.T

    df = pd.concat(frames, axis=1)
    df.columns = ["_".join(map(str, col)).strip("_") for col in df.columns]
    df = df.reset_index().rename(columns={"index": "code"})
    return df


def main() -> None:
    print(f"[INFO]  {CODES} {PERIOD} {START}~{END} count={COUNT}")
    kwargs = dict(
        field_list=[],
        stock_list=CODES,
        period=PERIOD,
        start_time=START,
        end_time=END,
        count=COUNT,
        dividend_type="none",
        fill_data=False,
    )

    data_dict: Dict[str, Any] | None = None
    if hasattr(xtdata, "get_market_data_ex"):
        try:
            data_dict = xtdata.get_market_data_ex(**kwargs)
        except TypeError:
            pass
    if data_dict is None:
        data_dict = xtdata.get_market_data(**kwargs)

    if not isinstance(data_dict, dict) or not data_dict:
        raise RuntimeError("xtdata ؿݣʱ䴰ںͱػ")

    df = _normalize_dataframe(data_dict)
    df.to_csv(SAVE_PATH, index=False, encoding="utf-8-sig")
    print(f"[DONE] д {len(df)} ¼ {SAVE_PATH}")


if __name__ == "__main__":
    main()
"""
Path('scripts/archive/dump_xtdata_csv.py').write_text(content, encoding='utf-8')
