"""akdata 客户端 — 一行代码取 A 股全市场行情数据"""

import os
import subprocess
from pathlib import Path

import duckdb
import pandas as pd


class DataHub:
    """A 股行情数据入口"""

    def __init__(self, repo_url: str = "https://github.com/kai-aii/akdata.git",
                 cache_dir: str = None):
        self.repo_url = repo_url
        self.cache_dir = Path(cache_dir or os.path.expanduser("~/.akdata_cache"))
        self.data_dir = self.cache_dir / "data"

        if not (self.cache_dir / ".git").exists():
            self._clone()
        else:
            self._pull()

    def _clone(self):
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "clone", "--depth", "1", self.repo_url, str(self.cache_dir)],
                       check=True)

    def _pull(self):
        subprocess.run(["git", "-C", str(self.cache_dir), "pull", "--depth", "1"],
                       check=True)

    def daily(self, symbols: list[str] = None, start: str = None,
              end: str = None) -> pd.DataFrame:
        """日线 OHLCV"""
        sql = "SELECT * FROM read_parquet('{}')".format(
            str((self.data_dir / "daily" / "*.parquet").as_posix()))
        conditions = []
        if symbols:
            codes = ",".join(f"'{s.zfill(6)}'" for s in symbols)
            conditions.append(f"symbol IN ({codes})")
        if start:
            conditions.append(f"date >= '{start}'")
        if end:
            conditions.append(f"date <= '{end}'")
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY symbol, date"
        return duckdb.sql(sql).df()

    def snapshot(self, date: str) -> pd.DataFrame:
        """全市场某日快照"""
        return duckdb.sql(f"""
            SELECT * FROM read_parquet('{str((self.data_dir/"daily/*.parquet").as_posix())}')
            WHERE date = '{date}'
        """).df()

    def query(self, sql: str) -> pd.DataFrame:
        """写 DuckDB SQL"""
        return duckdb.sql(sql).df()

    def symbols(self) -> pd.DataFrame:
        return pd.read_parquet(self.data_dir / "meta" / "symbols.parquet")

