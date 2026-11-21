"""画像调度输入分区清单：按作业窗口调度到达分区，保证跨天分区恰好计入一次。

补齐画像流水线"按上游依赖启动"的能力：

- 上游依赖声明（expect）：运行窗声明等待的分区，依赖就绪即可触发画像作业，
  不必死等关窗时间；
- 调度输入分区清单（manifest）：分区到达时按运行窗分配，封窗后清单冻结，
  画像作业只消费清单确定的分区；
- 恰好一次：同一分区重复提交、到达消息重放都只算一次；跨天到达归入哪个
  运行窗，由 explain() 给出可核对的理由；
- 可追溯：清单条目带来源批次，画像的行数与空值比例可按分区回溯。
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Iterable

WINDOW_OPEN = "OPEN"
WINDOW_SEALED = "SEALED"


def _fmt(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


class NoOpenWindowError(Exception):
    """没有可接收该到达分区的开放运行窗。"""


class UnknownWindowError(KeyError):
    """运行窗不存在。"""


@dataclass(frozen=True)
class PartitionArrival:
    """上游分区到达消息。rows 为该分区样本行，供画像作业统计。"""

    partition_id: str
    arrived_at: datetime
    batch_id: str
    rows: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class Assignment:
    """分区到运行窗的归属记录。replayed 标记本次提交是重复/消息重放。"""

    partition_id: str
    window_id: str
    batch_id: str
    arrived_at: datetime
    reason: str
    replayed: bool = False
    batch_conflict: bool = False  # 重放批次与首次登记批次不一致


@dataclass(frozen=True)
class ManifestEntry:
    """调度输入分区清单条目：分区 + 来源批次 + 到达时间。"""

    partition_id: str
    batch_id: str
    arrived_at: datetime


@dataclass(frozen=True)
class Manifest:
    """封窗时冻结的调度输入分区清单，画像作业只消费清单内的分区。"""

    window_id: str
    business_date: str
    sealed_at: datetime
    entries: tuple[ManifestEntry, ...]

    @property
    def partition_ids(self) -> tuple[str, ...]:
        return tuple(e.partition_id for e in self.entries)


@dataclass
class RunWindow:
    """画像作业运行窗：[open_at, close_at) 内接收到达分区。"""

    window_id: str
    business_date: str
    open_at: datetime
    close_at: datetime
    expected: set[str] = field(default_factory=set)
    state: str = WINDOW_OPEN

    def covers(self, ts: datetime) -> bool:
        return self.open_at <= ts < self.close_at


class ProfileDispatchService:
    """画像调度器：把到达分区分配进恰好一个运行窗，并产出调度输入分区清单。"""

    def __init__(self) -> None:
        self._windows: dict[str, RunWindow] = {}
        self._assignments: dict[str, Assignment] = {}
        self._entries: dict[str, list[ManifestEntry]] = {}
        self._manifests: dict[str, Manifest] = {}
        self._rows: dict[str, tuple[dict[str, Any], ...]] = {}

    # ── 运行窗与上游依赖 ──────────────────────────────────────

    def register_window(
        self,
        window_id: str,
        business_date: str,
        open_at: datetime,
        close_at: datetime,
        expected: Iterable[str] = (),
    ) -> RunWindow:
        """注册运行窗并声明上游依赖分区（expected）。"""
        if window_id in self._windows:
            raise ValueError(f"运行窗 {window_id} 已注册")
        if not open_at < close_at:
            raise ValueError(f"运行窗 {window_id} 开窗时间必须早于关窗时间")
        window = RunWindow(window_id, business_date, open_at, close_at, set(expected))
        self._windows[window_id] = window
        self._entries[window_id] = []
        return window

    def expect(self, window_id: str, partition_id: str) -> None:
        """为运行窗追加上游依赖声明（两次调度可以等同一份迟到的分区）。"""
        self._window(window_id).expected.add(partition_id)

    def pending_expected(self, window_id: str) -> list[str]:
        """运行窗仍在等待的上游分区。"""
        window = self._window(window_id)
        assigned = {e.partition_id for e in self._entries[window_id]}
        return sorted(window.expected - assigned)

    # ── 到达分配（幂等，重复提交/重放只算一次） ─────────────────

    def on_arrival(self, arrival: PartitionArrival) -> Assignment:
        """处理分区到达消息：首次到达分配运行窗，重复提交/重放返回原归属。"""
        existing = self._assignments.get(arrival.partition_id)
        if existing is not None:
            return replace(
                existing,
                replayed=True,
                batch_conflict=existing.batch_id != arrival.batch_id,
            )
        window, reason = self._select_window(arrival)
        assignment = Assignment(
            partition_id=arrival.partition_id,
            window_id=window.window_id,
            batch_id=arrival.batch_id,
            arrived_at=arrival.arrived_at,
            reason=reason,
        )
        self._assignments[arrival.partition_id] = assignment
        self._entries[window.window_id].append(
            ManifestEntry(arrival.partition_id, arrival.batch_id, arrival.arrived_at)
        )
        self._rows[arrival.partition_id] = tuple(arrival.rows)
        return assignment

    def _select_window(self, arrival: PartitionArrival) -> tuple[RunWindow, str]:
        """确定归属窗口并给出可解释理由：覆盖到达时间的开放窗里取最早关窗者；
        原属窗已封闭则顺延到下一开放窗。"""
        covering = [w for w in self._windows.values() if w.covers(arrival.arrived_at)]
        open_covering = [w for w in covering if w.state == WINDOW_OPEN]
        ts = _fmt(arrival.arrived_at)
        if open_covering:
            chosen = min(open_covering, key=lambda w: (w.close_at, w.open_at, w.window_id))
            reason = (
                f"到达 {ts} 落在 {chosen.window_id} "
                f"[{_fmt(chosen.open_at)}, {_fmt(chosen.close_at)}) 内；"
                f"{len(open_covering)} 个开放窗同时可接收时按最早关窗归属"
            )
            return chosen, reason
        if covering:
            sealed_ids = "、".join(sorted(w.window_id for w in covering))
            candidates = [
                w
                for w in self._windows.values()
                if w.state == WINDOW_OPEN and w.close_at > arrival.arrived_at
            ]
            if candidates:
                chosen = min(candidates, key=lambda w: (w.close_at, w.open_at, w.window_id))
                reason = (
                    f"到达 {ts} 原属窗口 {sealed_ids} 已封闭；"
                    f"跨天到达顺延至下一开放窗 {chosen.window_id}（{_fmt(chosen.close_at)} 关窗）"
                )
                return chosen, reason
            raise NoOpenWindowError(f"到达 {ts} 的原属窗口 {sealed_ids} 已封闭且无后续开放窗")
        raise NoOpenWindowError(f"到达 {ts} 不在任何已注册运行窗内")

    # ── 依赖驱动启动 ──────────────────────────────────────────

    def due_windows(self, now: datetime) -> list[str]:
        """可启动的运行窗：有输入且上游依赖全部到达，或已到关窗时间。"""
        due = []
        for window in self._windows.values():
            if window.state != WINDOW_OPEN:
                continue
            entries = self._entries[window.window_id]
            assigned = {e.partition_id for e in entries}
            deps_met = window.expected <= assigned
            if (entries and deps_met) or now >= window.close_at:
                due.append(window)
        return [w.window_id for w in sorted(due, key=lambda w: (w.close_at, w.window_id))]

    # ── 封窗与调度输入分区清单 ────────────────────────────────

    def seal(self, window_id: str, sealed_at: datetime | None = None) -> Manifest:
        """封窗并冻结调度输入分区清单（幂等：重复封窗返回同一份清单）。"""
        if window_id in self._manifests:
            return self._manifests[window_id]
        window = self._window(window_id)
        window.state = WINDOW_SEALED
        manifest = Manifest(
            window_id=window_id,
            business_date=window.business_date,
            sealed_at=sealed_at or datetime.now(),
            entries=tuple(self._entries[window_id]),
        )
        self._manifests[window_id] = manifest
        return manifest

    def manifest(self, window_id: str) -> Manifest | None:
        """已封窗则返回冻结清单，未封窗返回 None。"""
        return self._manifests.get(window_id)

    def manifest_entries(self, window_id: str) -> tuple[ManifestEntry, ...]:
        """查看运行窗当前清单条目（封窗前随到达增长）。"""
        self._window(window_id)
        return tuple(self._entries[window_id])

    # ── 归属解释与样本读取 ────────────────────────────────────

    def explain(self, partition_id: str) -> Assignment | None:
        """解释分区归入了哪个运行窗、为什么（跨天到达必须可解释）。"""
        return self._assignments.get(partition_id)

    def partition_rows(self, partition_id: str) -> tuple[dict[str, Any], ...]:
        """画像作业按清单条目读取分区样本行。"""
        if partition_id not in self._rows:
            raise KeyError(f"未知分区 {partition_id}")
        return self._rows[partition_id]

    def _window(self, window_id: str) -> RunWindow:
        try:
            return self._windows[window_id]
        except KeyError:
            raise UnknownWindowError(f"未知运行窗 {window_id}") from None
