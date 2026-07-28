"""数据清洗与校验 — Parquet Schema + Pandera

校验两层：
  1. Parquet Schema：写入时自动验类型（pa.schema）
  2. Pandera Schema：值域 + 逻辑校验（@pda.check_input）
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pandera.pandas as pda
import tempfile, os


# ===== 1. Parquet Schema（类型校验）=====

DAILY_SCHEMA = pa.schema([
    ('symbol', pa.string()),
    ('date',   pa.date32()),
    ('open',   pa.float64()),
    ('high',   pa.float64()),
    ('low',    pa.float64()),
    ('close',  pa.float64()),
    ('volume', pa.int64()),
    ('amount', pa.float64()),
])

# ===== 2. Pandera Schema（值域+逻辑校验）=====

DAILY_PANDERA = pda.DataFrameSchema({
    'symbol': pda.Column(str),
    'date':   pda.Column(pda.DateTime),
    'open':   pda.Column(float, pda.Check.ge(0)),
    'high':   pda.Column(float, pda.Check.ge(0)),
    'low':    pda.Column(float, pda.Check.ge(0)),
    'close':  pda.Column(float, pda.Check.ge(0)),
    'volume': pda.Column(int,   pda.Check.ge(0)),
    'amount': pda.Column(float, pda.Check.ge(0)),
})

# ===== 2.5 指数 Schema =====

INDEX_SCHEMA = pa.schema([
    ('code',   pa.string()),
    ('name',   pa.string()),
    ('date',   pa.date32()),
    ('open',   pa.float64()),
    ('high',   pa.float64()),
    ('low',    pa.float64()),
    ('close',  pa.float64()),
    ('volume', pa.int64()),
    ('amount', pa.float64()),
])

INDEX_PANDERA = pda.DataFrameSchema({
    'code':   pda.Column(str),
    'name':   pda.Column(str),
    'date':   pda.Column(pda.DateTime),
    'open':   pda.Column(float, pda.Check.ge(0)),
    'high':   pda.Column(float, pda.Check.ge(0)),
    'low':    pda.Column(float, pda.Check.ge(0)),
    'close':  pda.Column(float, pda.Check.ge(0)),
    'volume': pda.Column(int,   pda.Check.ge(0)),
    'amount': pda.Column(float, pda.Check.ge(0)),
})


# ===== 3. 清洗转换 =====

def raw_bars_to_df(raw_bars: list, symbol: str) -> pd.DataFrame:
    """pytdx get_security_bars 原始输出 → 标准 DataFrame

    pytdx 返回的每个 bar 字段：
    ['open','close','high','low','vol','amount','year','month','day','hour','minute','datetime']
    """
    if not raw_bars:
        return pd.DataFrame(columns=['symbol','date','open','high','low','close','volume','amount'])
    df = pd.DataFrame(raw_bars)

    # symbol
    df['symbol'] = str(symbol).zfill(6)

    # date：year+month+day → datetime（容错：异常日期置 NaT 后剔除）
    df['date'] = pd.to_datetime(
        df['year'].astype(str) + '-' +
        df['month'].astype(str).str.zfill(2) + '-' +
        df['day'].astype(str).str.zfill(2),
        format='mixed',
        errors='coerce',
    )
    bad_dates = df['date'].isna().sum()
    if bad_dates > 0:
        print(f"    ⚠️ {symbol}: {bad_dates} 行日期解析失败，已剔除")
    df = df.dropna(subset=['date'])

    # 重命名 + 选列
    df = df.rename(columns={'vol': 'volume'})

    # 类型强转（pytdx 的 vol 是 float，必须转 int64）
    df['volume'] = df['volume'].fillna(0).astype('int64')
    df['amount'] = df['amount'].fillna(0).astype(float)

    return df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount']]


@pda.check_input(DAILY_PANDERA)
def validate_daily(df: pd.DataFrame) -> pd.DataFrame:
    """校验 + 去重 + 排序"""
    df = df.drop_duplicates(subset=['symbol', 'date'], keep='last')
    df = df.sort_values(['symbol', 'date']).reset_index(drop=True)
    return df


# ===== 4. 写入 =====

def save_daily(df: pd.DataFrame, path: str):
    """写入日线 Parquet（原子写入）"""
    df['date'] = pd.to_datetime(df['date']).astype('datetime64[us]')
    table = pa.Table.from_pandas(df, schema=DAILY_SCHEMA)
    _atomic_write(table, path)


def save_index(df: pd.DataFrame, path: str):
    """写入指数 Parquet（原子写入）"""
    df['date'] = pd.to_datetime(df['date']).astype('datetime64[us]')
    table = pa.Table.from_pandas(df, schema=INDEX_SCHEMA)
    _atomic_write(table, path)


def _atomic_write(table: pa.Table, target: str):
    """原子写入：先写临时文件，再 os.replace（同盘原子 rename）"""
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(target), suffix='.parquet')
    os.close(fd)
    try:
        pq.write_table(table, tmp)
        os.replace(tmp, target)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ===== 5. 股票列表 =====

SYMBOLS_SCHEMA = pa.schema([
    ('code', pa.string()),
    ('name', pa.string()),
])

SYMBOLS_PANDERA = pda.DataFrameSchema({
    'code': pda.Column(str, pda.Check.str_matches(r'^\d{6}$')),
    'name': pda.Column(str),
})


def save_symbols(df: pd.DataFrame, path: str):
    """写入股票列表 Parquet（原子写入）"""
    df = SYMBOLS_PANDERA.validate(df)
    table = pa.Table.from_pandas(df, schema=SYMBOLS_SCHEMA)
    _atomic_write(table, path)
