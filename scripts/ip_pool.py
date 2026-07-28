"""IP 池管理 + 连接 failover

实测数据（2026-07-11）：
  静态 IP：18 个（100% 可用）
  DNS 解析增量：16 个（100% 可用）
  总池：34 个
  最低延迟：82ms  最高延迟：397ms
  GitHub Actions：34/34 全通
"""

import socket
from pytdx.hq import TdxHq_API

STATIC_IPS = [
    ('114.28.173.142', 7709), ('103.251.85.94', 7709),
    ('115.238.90.165', 7709), ('115.238.56.198', 7709),
    ('180.153.18.170', 7709), ('218.75.126.9', 7709),
    ('60.191.117.167', 7709), ('123.125.108.90', 7709),
    ('60.12.136.250', 7709), ('103.221.142.65', 7709),
    ('103.221.142.68', 7709), ('103.221.142.80', 7709),
    ('139.9.43.104', 7709), ('139.9.50.246', 7709),
    ('139.9.43.31', 7709), ('103.221.142.66', 7709),
    ('103.221.142.67', 7709), ('103.221.142.73', 7709),
]

DYNAMIC_HOSTS = [
    'shtdx.gtjas.com',    # 国泰君安上海
    'jstdx.gtjas.com',    # 国泰君安通用
    'sztdx.gtjas.com',    # 国泰君安深圳
]


def build_pool() -> list:
    """构建 IP 池：静态 + DNS 动态解析"""
    pool = list(STATIC_IPS)
    for host in DYNAMIC_HOSTS:
        try:
            for info in socket.getaddrinfo(host, 7709, socket.AF_INET):
                ip = info[4][0]
                if (ip, 7709) not in pool:
                    pool.append((ip, 7709))
        except Exception:
            pass
    return pool


def connect_with_retry(timeout=3, max_rounds=3, pool=None) -> tuple[TdxHq_API, str]:
    """连接 IP 池，返回第一个可用的 (api, ip)

    Args:
        timeout: 单个 IP 连接超时秒数（默认 3s）
        max_rounds: 全部不可达时重试轮数
        pool: 预构建 IP 列表，传入时跳过 build_pool()

    Raises:
        ConnectionError: 所有 IP 全部不可达
    """
    import time

    if pool is None:
        pool = build_pool()
    for round_num in range(max_rounds):
        for ip, port in pool:
            api = TdxHq_API(heartbeat=True)
            try:
                if api.connect(ip, port, time_out=timeout):
                    return api, ip
                try:
                    api.disconnect()
                except Exception:
                    pass
            except Exception:
                try:
                    api.disconnect()
                except Exception:
                    pass
        if round_num < max_rounds - 1:
            time.sleep(2)

    raise ConnectionError(f"{len(pool)} 个 IP 全部不可达（已重试 {max_rounds} 轮）")
