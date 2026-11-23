"""共享分区日期计算：统一「保留天数 → 到期边界 → 分区值/带表名路径」的推导。

分区管理的过期分区预览和表归档的归档计划此前各写了一套日期推导，
两边的基准日、边界包含关系和格式都容易漂移。这里把这段计算收拢成
唯一实现，两处只保留各自的输出格式和数量上限。

约定：
- ``as_of`` 默认取平台示例数据的基准日，保持既有接口输出稳定；
  调用方可以显式传入日期，便于签发/提交时固定留存依据。
- 保留天数 N 对应的到期边界是 ``as_of - N 天``，与原实现一致。
- 到期边界当天按已达保留期处理：``age >= N`` 即过期，可迁出；
  年龄小于 N 的日期仍受保护。淘汰窗口从边界当天开始，和原归档清单一致。
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Iterable

# 平台示例数据统一使用的基准日。原来的两处实现都硬编码 2026-11-21，
# 这里收拢后保持同一基准，避免旧接口输出发生变化。
REFERENCE_DATE = date(2026, 11, 21)

# 分区预览最多列出 30 个分区；归档计划沿用归档侧 15 个日期的窗口上限。
PREVIEW_PARTITION_LIMIT = 30
ARCHIVE_PARTITION_LIMIT = 15


def resolve_as_of(as_of: date | datetime | str | None = None) -> date:
    """把不同形态的基准日归一成 ``date``。"""
    if as_of is None:
        return REFERENCE_DATE
    if isinstance(as_of, datetime):
        return as_of.date()
    if isinstance(as_of, date):
        return as_of
    text = str(as_of).strip()
    if not text:
        return REFERENCE_DATE
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"无法识别的基准日期：{as_of!r}")


def expiry_cutoff(retention_days: int, as_of: date | datetime | str | None = None) -> date:
    """返回保留 N 天对应的到期边界日期。"""
    return resolve_as_of(as_of) - timedelta(days=int(retention_days))


def dates_from_cutoff(
    cutoff: date | datetime | str,
    limit: int = ARCHIVE_PARTITION_LIMIT,
) -> list[date]:
    """从到期边界开始向前逐日展开，返回最多 ``limit`` 个日期。

    顺序与两处原实现一致：先到期边界当天，再逐日向更早推进。
    """
    start = resolve_as_of(cutoff)
    count = max(int(limit), 0)
    return [start - timedelta(days=index) for index in range(count)]


def expired_dates(
    retention_days: int,
    *,
    limit: int = ARCHIVE_PARTITION_LIMIT,
    as_of: date | datetime | str | None = None,
) -> list[date]:
    """按保留天数取得到期日期序列（含到期边界当天）。"""
    return dates_from_cutoff(expiry_cutoff(retention_days, as_of), limit)


def partition_age(
    value: date | datetime | str,
    as_of: date | datetime | str | None = None,
) -> int:
    """计算分区日期相对基准日的年龄（天）。"""
    return (resolve_as_of(as_of) - resolve_as_of(value)).days


def is_expired(
    value: date | datetime | str,
    retention_days: int,
    as_of: date | datetime | str | None = None,
) -> bool:
    """分区是否已达保留期边界：年龄大于或等于保留天数。

    边界当天计入过期；保留期越长，边界越早、受保护的日期越多。
    """
    return partition_age(value, as_of) >= int(retention_days)


def is_protected(
    value: date | datetime | str,
    retention_days: int,
    as_of: date | datetime | str | None = None,
) -> bool:
    """分区是否仍在保留期内（受保护，不得迁出）：年龄小于保留天数。"""
    return not is_expired(value, retention_days, as_of)


def format_partition_value(value: date | datetime | str, granularity: str = "day") -> str:
    """按原分区预览的粒度格式输出分区值。

    day/month/hour/year 的格式与分区管理原实现逐字一致；未知粒度按 year 处理。
    """
    current = resolve_as_of(value)
    if granularity == "day":
        return current.strftime("dt=%Y-%m-%d")
    if granularity == "month":
        return current.strftime("dt=%Y-%m")
    if granularity == "hour":
        return current.strftime("dt=%Y-%m-%d-%H")
    return current.strftime("dt=%Y")


def format_partition_path(
    table: str,
    value: date | datetime | str,
    granularity: str = "day",
) -> str:
    """输出带表名的归档路径，格式与原归档计划一致：``表名/dt=YYYY-MM-DD``。"""
    return f"{table}/{format_partition_value(value, granularity)}"


_PARTITION_DATE_PATTERNS = (
    re.compile(r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})(?:-\d{2})?$"),
    re.compile(r"(?P<year>\d{4})-(?P<month>\d{2})$"),
    re.compile(r"(?P<year>\d{4})$"),
)


def parse_partition_date(value: date | datetime | str) -> date:
    """从 ``dt=...``、``表名/dt=...`` 或纯日期文本中解析出分区日期。"""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        raise ValueError("分区日期为空")
    tail = text.rsplit("/", 1)[-1]
    if "=" in tail:
        tail = tail.split("=", 1)[-1]
    for pattern in _PARTITION_DATE_PATTERNS:
        match = pattern.search(tail)
        if match is None:
            continue
        year = int(match.group("year"))
        month = int(match.groupdict().get("month") or 1)
        day = int(match.groupdict().get("day") or 1)
        return date(year, month, day)
    raise ValueError(f"无法识别的分区日期：{value!r}")


def unique_dates(values: Iterable[date]) -> list[date]:
    """按输入顺序去重，保留原清单顺序。"""
    seen: set[date] = set()
    result: list[date] = []
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result
