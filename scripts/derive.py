"""周线派生 — 从日线 Parquet 通过 DuckDB SQL 聚合生成"""

import duckdb
from pathlib import Path


def _derive_period(daily_dir: str, out_dir: str, period: str):
    """从日线派生指定周期聚合"""
    daily_glob = str(Path(daily_dir).as_posix()) + "/*.parquet"
    out = str(Path(out_dir).as_posix())
    with duckdb.connect() as con:
        con.execute(f"""
            CREATE OR REPLACE TABLE agg AS
            SELECT
                symbol,
                DATE_TRUNC('{period}', date) AS date,
                FIRST(open)   AS open,
                MAX(high)     AS high,
                MIN(low)      AS low,
                LAST(close)   AS close,
                SUM(volume)   AS volume,
                SUM(amount)   AS amount
            FROM read_parquet(?)
            GROUP BY symbol, DATE_TRUNC('{period}', date)
            ORDER BY symbol, date
        """, [daily_glob])
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        con.execute(
            "COPY (SELECT * FROM agg) TO ? "
            "(FORMAT PARQUET, PARTITION_BY (symbol), OVERWRITE_OR_IGNORE)",
            [out]
        )


def derive_all(data_dir: str):
    """派生周线"""
    base = Path(data_dir)
    daily = str(base / "daily")
    _derive_period(daily, str(base / "weekly"), "week")
    print("周线派生完成")
