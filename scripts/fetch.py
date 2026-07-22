"""
akdata 核心采集脚本

用法：
    python fetch.py                  # 日常更新
    python fetch.py --init           # 初始化拉全量
    python fetch.py --date 2026-07-03
    python fetch.py --from-date 2026-06-01 --to-date 2026-06-30
    python fetch.py --symbols 000001,600519
"""

import argparse
from pathlib import Path
import sys
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import traceback

# 确保能从项目根或 scripts/ 目录运行
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

import pandas as pd
from pytdx.params import TDXParams
from pytdx.hq import TdxHq_API

from ip_pool import connect_with_retry
from clean import raw_bars_to_df, validate_daily, save_daily, \
    save_index, save_symbols, INDEX_PANDERA, DAILY_PANDERA


DATA_DIR = Path(__file__).resolve().parent.parent / "data"   # akdata/data/
START_DATE = "2017-01-01"


# ===== A. 股票列表 =====

def is_a_stock(s: dict) -> bool:
    """过滤出 A 股（排除 B 股、债券、基金、指数、板块等）"""
    code = str(s.get('code', ''))
    name = str(s.get('name', ''))

    # 必须是 6 位纯数字
    if not code.isdigit() or len(code) != 6:
        return False

    # 上海 A 股：60xxxx, 68xxxx
    if code.startswith(('60', '68')):
        return True

    # 深圳 A 股：00xxxx, 30xxxx（排除 39xxxx 指数、395xxx 板块）
    if code.startswith(('00', '30')) and not code.startswith('39'):
        return True

    return False


def fetch_stock_list(api) -> list[dict]:
    """获取沪深全量 A 股列表"""
    stocks = []

    # 上海：start=0 返回空，必须从 1000 开始
    for start in range(1000, 50000, 1000):
        batch = api.get_security_list(1, start)
        if not batch:
            break
        stocks.extend(batch)

    # 深圳：分页有大空洞（7000-14999 连续 8 页空），容忍更多连续空
    empty_streak = 0
    max_empty_streak = 12    # 容忍最多 12 次连续空（12×1000=12000 条）
    for start in range(0, 50000, 1000):
        batch = api.get_security_list(0, start)
        if not batch:
            empty_streak += 1
            if empty_streak >= max_empty_streak:
                break
            continue
        empty_streak = 0
        stocks.extend(batch)

    return [s for s in stocks if is_a_stock(s)]


# ===== B. 日线采集 =====

def fetch_daily_history(api, symbol: str) -> list:
    """拉取单只股票完整日线（从最早到最新）"""
    all_bars = []
    pos = 0
    market = 0 if symbol.startswith(('0', '3')) else 1

    while True:
        batch = api.get_security_bars(
            TDXParams.KLINE_TYPE_DAILY, market, symbol, pos, 800,
        )
        if not batch:
            break
        all_bars.extend(batch)
        if len(batch) < 800:
            break
        pos += 800

    # 截断到 START_DATE（字符串比较比 pd.Timestamp 快 10x）
    filtered = []
    for b in all_bars:
        if f"{b['year']:04d}-{b['month']:02d}-{b['day']:02d}" >= START_DATE:
            filtered.append(b)
    return filtered


def fetch_daily_incremental(api, symbol: str, last_date: str) -> pd.DataFrame:
    """增量拉取：只拉最近 800 根 K 线，过滤出新数据（800 根 ≈ 3 年）"""
    market = 0 if symbol.startswith(('0', '3')) else 1
    bars = api.get_security_bars(
        TDXParams.KLINE_TYPE_DAILY, market, symbol, 0, 800,
    )
    if not bars:
        return pd.DataFrame()
    df = raw_bars_to_df(bars, symbol)
    # 过滤出新数据 + 遵守 START_DATE 底线
    cutoff = max(pd.Timestamp(last_date), pd.Timestamp(START_DATE))
    return df[df['date'] > cutoff]


# ===== C. 指数 =====

INDEX_CODES = [
    (1, '000001', '上证指数'), (0, '399001', '深证成指'),
    (0, '399006', '创业板指'), (1, '000688', '科创50'),
    (1, '000300', '沪深300'),  (1, '000016', '上证50'),
    (0, '399005', '中小100'),  (1, '000852', '中证1000'),
]


def fetch_all_indexes(api) -> pd.DataFrame:
    """拉取全部指数日线（分页，每次 800 条）"""
    rows = []
    for market, code, name in INDEX_CODES:
        all_bars = []
        pos = 0
        while True:
            batch = api.get_index_bars(
                TDXParams.KLINE_TYPE_DAILY, market, code, pos, 800,
            )
            if not batch:
                break
            all_bars.extend(batch)
            if len(batch) < 800:
                break
            pos += 800
        for b in all_bars:
            rows.append({
                'code': code,
                'name': name,
                'date': f"{b['year']}-{b['month']:02d}-{b['day']:02d}",
                'open': b['open'],
                'high': b['high'],
                'low': b['low'],
                'close': b['close'],
                'volume': int(b['vol']),
                'amount': float(b.get('amount', 0)),
            })
    idx_df = pd.DataFrame(rows)
    if len(idx_df) > 0:
        idx_df = idx_df[idx_df['date'] >= START_DATE]
    return idx_df


# ===== F. 共用工具 =====

MAX_WORKERS = 20

def _hash_ip(symbol: str, pool_ips) -> tuple:
    return pool_ips[int(hashlib.md5(symbol.encode()).hexdigest(), 16) % len(pool_ips)]


def _connect_for(api_cls, ip, timeout=5):
    api = api_cls(heartbeat=True)
    if api.connect(ip[0], ip[1], time_out=timeout):
        return api
    try:
        api.disconnect()
    except Exception:
        pass
    return None


def _fetch_indexes():
    api, _ = connect_with_retry()
    try:
        idx = fetch_all_indexes(api)
        return idx
    finally:
        try:
            api.disconnect()
        except Exception:
            pass


def _save_symbols_list(stocks, path):
    df = pd.DataFrame([{'code': str(s['code']).zfill(6), 'name': s.get('name', '')} for s in stocks])
    save_symbols(df, str(path))
    return df


# ===== G. 入口 =====

def run_init():
    """模式 A：初始化 — 全量拉取所有 A 股日线"""
    api, ip = connect_with_retry()
    print(f"已连接: {ip}")

    stocks = fetch_stock_list(api)
    print(f"共 {len(stocks)} 只 A 股")

    (DATA_DIR / "daily").mkdir(parents=True, exist_ok=True)

    for i, s in enumerate(stocks):
        symbol = str(s['code']).zfill(6)
        name = s.get('name', '')
        print(f"[{i+1}/{len(stocks)}] {symbol} {name}")

        bars = fetch_daily_history(api, symbol)
        if not bars:
            # 可能是连接断了，尝试重连
            try:
                api.disconnect()
            except Exception:
                pass
            api, ip = connect_with_retry()
            print(f"    重连: {ip}")
            bars = fetch_daily_history(api, symbol)
        if not bars:
            print(f"    ⚠️ 无数据，跳过")
            continue
        df = raw_bars_to_df(bars, symbol)
        df = validate_daily(df)
        save_daily(df, str(DATA_DIR / "daily" / f"{symbol}.parquet"))

    api.disconnect()

    # 指数日线
    print(f"拉取指数...")
    idx_df = _fetch_indexes()
    if len(idx_df) > 0:
        idx_df['date'] = pd.to_datetime(idx_df['date'])
        (DATA_DIR / "index").mkdir(parents=True, exist_ok=True)
        idx_df = INDEX_PANDERA.validate(idx_df)
        save_index(idx_df, str(DATA_DIR / "index" / "indexes.parquet"))
        print(f"  指数: {len(idx_df)} 条")
    else:
        print(f"  指数: 无数据，跳过")

    # 股票列表
    (DATA_DIR / "meta").mkdir(parents=True, exist_ok=True)
    symbols_df = _save_symbols_list(stocks, DATA_DIR / "meta" / "symbols.parquet")
    print(f"  股票列表: {len(symbols_df)} 只")

    # 派生周线
    from derive import derive_all
    derive_all(str(DATA_DIR))

    print("初始化完成")


def run_update():
    """模式 B：日常更新 — 增量更新 + 自动发现新股"""
    api, ip = connect_with_retry()
    print(f"已连接: {ip}")

    daily_dir = DATA_DIR / "daily"
    existing = {p.stem for p in daily_dir.glob("*.parquet")}

    # 发现新股
    stocks = fetch_stock_list(api)
    all_codes = {str(s['code']).zfill(6) for s in stocks}
    new_codes = all_codes - existing
    if new_codes:
        print(f"发现 {len(new_codes)} 只新股，拉取全量...")
        for symbol in sorted(new_codes):
            bars = fetch_daily_history(api, symbol)
            if not bars:
                print(f"  ⚠️ {symbol}: 无数据，跳过")
                continue
            df = raw_bars_to_df(bars, symbol)
            df = validate_daily(df)
            save_daily(df, str(daily_dir / f"{symbol}.parquet"))
            print(f"  ✅ {symbol}: {len(df)} 条")

    api.disconnect()

    # 增量循环：多线程并发
    from ip_pool import build_pool
    pool_ips = tuple(build_pool())  # 冻结为不可变，避免闭包副作用
    if not pool_ips:
        raise RuntimeError("IP 池为空，无法继续")
    daily_files = sorted(daily_dir.glob("*.parquet"))
    print(f"增量更新 {len(daily_files)} 只股票 ({len(pool_ips)} IP)...")
    progress_lock = threading.Lock()
    failed = [0]

    def _fetch_one_daily(fpath):
        """单线程：独立连接，拉取一只股票的增量"""
        symbol = fpath.stem
        try:
            df_existing = pd.read_parquet(fpath)
        except Exception:
            try:
                fpath.unlink()
            except OSError:
                pass
            return symbol, None, "read_error"
        last_date = str(df_existing['date'].max())
        from ip_pool import connect_with_retry as _cwr
        try:
            api, ip = _cwr(timeout=5, pool=pool_ips)
        except (ConnectionError, OSError):
            return symbol, None, "connect_fail"
        try:
            df_new = fetch_daily_incremental(api, symbol, last_date)
            if len(df_new) == 0:
                return symbol, None, None
            return symbol, df_new, df_existing
        except Exception:
            traceback.print_exc()
            return symbol, None, "exception"
        finally:
            try:
                api.disconnect()
            except Exception:
                pass

    updated = 0
    with ThreadPoolExecutor(max_workers=min(20, len(pool_ips))) as executor:
        futures = {executor.submit(_fetch_one_daily, f): f for f in daily_files}
        for future in as_completed(futures):
            symbol, df_new, existing_or_err = future.result()
            if df_new is None:
                if existing_or_err:
                    with progress_lock:
                        failed[0] += 1
                continue
            if isinstance(existing_or_err, pd.DataFrame):
                df_existing = existing_or_err
            else:
                continue
            try:
                df_new = validate_daily(df_new)
            except Exception:
                with progress_lock:
                    failed[0] += 1
                continue
            df_merged = pd.concat([df_existing, df_new])
            df_merged = df_merged.drop_duplicates(subset=['symbol', 'date'], keep='last')
            df_merged['date'] = pd.to_datetime(df_merged['date'])
            df_merged = DAILY_PANDERA.validate(df_merged)
            save_daily(df_merged, str(daily_dir / f"{symbol}.parquet"))
            with progress_lock:
                updated += 1
                if (updated + failed[0]) % 500 == 0:
                    print(f"  进度: {updated + failed[0]}/{len(daily_files)}")

    if failed[0] > 0:
        print(f"  ⚠️ {failed[0]} 只失败（连接/解析错误）")

    # 指数日线增量
    print(f"拉取指数...")
    idx_df = _fetch_indexes()
    if len(idx_df) == 0:
        print(f"  指数: 无数据，跳过")
    else:
        idx_df['date'] = pd.to_datetime(idx_df['date'])
        (DATA_DIR / "index").mkdir(parents=True, exist_ok=True)
        idx_path = DATA_DIR / "index" / "indexes.parquet"
        if idx_path.exists():
            idx_old = pd.read_parquet(idx_path)
            idx_old['date'] = pd.to_datetime(idx_old['date'])
            idx_merged = pd.concat([idx_old, idx_df])
            idx_merged = idx_merged.drop_duplicates(subset=['code', 'date'], keep='last')
            idx_merged = INDEX_PANDERA.validate(idx_merged)
            save_index(idx_merged, str(idx_path))
        else:
            idx_df = INDEX_PANDERA.validate(idx_df)
            save_index(idx_df, str(idx_path))
        idx_new = len(idx_df)
        print(f"  指数: +{idx_new} 条")

    # 股票列表增量
    print(f"更新股票列表...")
    symbols_path = DATA_DIR / "meta" / "symbols.parquet"
    api_sl, ip_sl = connect_with_retry()
    stocks = fetch_stock_list(api_sl)
    api_sl.disconnect()
    if not stocks:
        print(f"  股票列表: 无数据，跳过")
    else:
        symbols_df = _save_symbols_list(stocks, symbols_path)
        print(f"  股票列表: {len(symbols_df)} 只")

    if updated > 0:
        from derive import derive_all
        derive_all(str(DATA_DIR))

    print(f"更新完成: {updated} 只股票有新数据")


def run_patch(date=None, from_date=None, to_date=None, symbols=None):
    """模式 C：手动补数"""
    (DATA_DIR / "daily").mkdir(parents=True, exist_ok=True)
    api, ip = connect_with_retry()
    print(f"已连接: {ip}")

    if symbols:
        symbol_list = [s.strip() for s in symbols.split(',')]
    else:
        # 未指定股票则补全部
        stocks = fetch_stock_list(api)
        symbol_list = [str(s['code']).zfill(6) for s in stocks]

    for symbol in symbol_list:
        bars = fetch_daily_history(api, symbol)
        if not bars:
            try:
                api.disconnect()
            except Exception:
                pass
            api, ip = connect_with_retry()
            print(f"    重连: {ip}")
            bars = fetch_daily_history(api, symbol)
        if not bars:
            print(f"  ⚠️ {symbol}: 无数据，跳过")
            continue
        df = raw_bars_to_df(bars, symbol)
        if date:
            df = df[df['date'] == pd.Timestamp(date)]
        elif from_date and to_date:
            df = df[(df['date'] >= pd.Timestamp(from_date)) &
                    (df['date'] <= pd.Timestamp(to_date))]
        if len(df) == 0:
            continue
        df = validate_daily(df)
        filepath = str(DATA_DIR / "daily" / f"{symbol}.parquet")
        if Path(filepath).exists():
            existing = pd.read_parquet(filepath)
            df = pd.concat([existing, df])
            df = df.drop_duplicates(subset=['symbol', 'date'], keep='last')
            df = df.sort_values(['symbol', 'date']).reset_index(drop=True)
        df['date'] = pd.to_datetime(df['date'])
        df = DAILY_PANDERA.validate(df)
        save_daily(df, filepath)
        print(f"  {symbol}: 写入 {len(df)} 条")

    api.disconnect()
    print("补数完成")


def main():
    parser = argparse.ArgumentParser(description="akdata 数据采集")
    parser.add_argument('--init', action='store_true',
                        help='初始化：拉全市场 10 年全量')
    parser.add_argument('--date', help='补数：指定日期 YYYY-MM-DD')
    parser.add_argument('--from-date', help='补数：起始日期')
    parser.add_argument('--to-date', help='补数：截止日期')
    parser.add_argument('--symbols', help='补数：股票代码，逗号分隔')
    args = parser.parse_args()

    if args.init:
        run_init()
    elif args.date or args.from_date or args.symbols:
        run_patch(args.date, args.from_date, args.to_date, args.symbols)
    else:
        run_update()


if __name__ == "__main__":
    main()
