"""画像调度输入分区清单：跨天分区恰好计入一次的端到端验证。

场景锚点：region_north 的末班样本 N17（20 行、4 行缺金额）凌晨 1 点才补到，
前晚与当日两个作业窗都有空位接收、两次调度都在等它；但追查两份画像时，
N17 对行数的总贡献只能是 20、对空金额个数的总贡献只能是 4。
"""
from datetime import datetime

import pytest

import app.store as store_mod
from app.services.profile_dispatch import (
    NoOpenWindowError,
    PartitionArrival,
    ProfileDispatchService,
    UnknownWindowError,
)
from app.services.profile_stats import ProfileStatsService

ASSET = "REGION_NORTH_DAILY"


def _rows(prefix: str, n: int, nulls: int) -> tuple:
    """造 n 行样本，前 nulls 行缺金额。"""
    return tuple(
        {
            "id": f"{prefix}-{i:02d}",
            "地区": "region_north",
            "金额": None if i < nulls else f"{100 + i}.00",
        }
        for i in range(n)
    )


def _fresh():
    """重置存储，返回全新的调度器与画像服务。"""
    store_mod.store.reset()
    return ProfileDispatchService(), ProfileStatsService()


def _register_overlapping_windows(dispatch: ProfileDispatchService) -> None:
    """前晚窗 18:00→次日02:00 与当日窗 00:00→18:00，两次调度都声明等 N17。"""
    dispatch.register_window(
        "WIN-NIGHT-1120", "2026-11-20",
        datetime(2026, 11, 20, 18, 0), datetime(2026, 11, 21, 2, 0),
        expected=["N16", "N17"],
    )
    dispatch.register_window(
        "WIN-DAY-1121", "2026-11-21",
        datetime(2026, 11, 21, 0, 0), datetime(2026, 11, 21, 18, 0),
        expected=["N17", "D01"],
    )


# ══════════════════════════════════════════════════════════════════
# 主场景：N17 凌晨 1 点补到，两个窗都有空位，重放后仍只计一次
# ══════════════════════════════════════════════════════════════════

class TestCrossDayPartitionExactlyOnce:

    def test_n17_contributes_20_rows_4_nulls_across_two_profiles(self):
        dispatch, profiler = _fresh()
        _register_overlapping_windows(dispatch)

        # 23:00 前晚窗常规分区先到（10 行、1 行缺金额）
        dispatch.on_arrival(PartitionArrival(
            "N16", datetime(2026, 11, 20, 23, 0), "BATCH-N16", _rows("N16", 10, 1)))
        # 凌晨 1 点 N17 补到：两窗都有空位，按最早关窗归入前晚窗
        n17 = PartitionArrival("N17", datetime(2026, 11, 21, 1, 0), "BATCH-N17", _rows("N17", 20, 4))
        assignment = dispatch.on_arrival(n17)
        assert assignment.window_id == "WIN-NIGHT-1120"
        assert assignment.replayed is False

        # 上游依赖就绪 → 01:05 即触发前晚画像作业（不必等 02:00 关窗）
        started = profiler.start_due_windows(dispatch, datetime(2026, 11, 21, 1, 5), ASSET)
        assert [r["window_id"] for r in started] == ["WIN-NIGHT-1120"]
        night = started[0]["profile"]
        assert night["行数统计"] == "30"            # N16 10 行 + N17 20 行
        assert night["空值比例"] == "0.1667"        # 5 / 30
        assert night["业务日期"] == "2026-11-20"

        # 两次运行之间重放到达消息：归属不变，不进当日清单
        replay = dispatch.on_arrival(n17)
        assert replay.replayed is True
        assert replay.window_id == "WIN-NIGHT-1120"
        day_entries = [e.partition_id for e in dispatch.manifest_entries("WIN-DAY-1121")]
        assert "N17" not in day_entries

        # 当日常规分区 10:00 到达，18:00 关窗触发当日画像
        dispatch.on_arrival(PartitionArrival(
            "D01", datetime(2026, 11, 21, 10, 0), "BATCH-D01", _rows("D01", 5, 0)))
        started = profiler.start_due_windows(dispatch, datetime(2026, 11, 21, 18, 0), ASSET)
        assert [r["window_id"] for r in started] == ["WIN-DAY-1121"]
        assert started[0]["profile"]["行数统计"] == "5"

        # 追查两份画像：N17 总贡献恰好 20 行 / 4 空金额，且只出现在一份画像
        recon = profiler.reconcile_partition(dispatch, "N17")
        assert recon["run_count"] == 1
        assert recon["exactly_once"] is True
        assert recon["total_rows"] == 20
        assert recon["total_nulls"] == 4
        assert recon["balanced"] is True

        # 画像使用方按日核对：N17 只计入 11-20，不在两天都计入
        d20 = profiler.daily_reconciliation("2026-11-20")
        d21 = profiler.daily_reconciliation("2026-11-21")
        assert (d20["rows"], d20["nulls"]) == (30, 5)
        assert (d21["rows"], d21["nulls"]) == (5, 0)
        assert "N17" in d20["partitions"]
        assert "N17" not in d21["partitions"]

    def test_cross_day_assignment_is_explainable(self):
        dispatch, _ = _fresh()
        _register_overlapping_windows(dispatch)
        assignment = dispatch.on_arrival(PartitionArrival(
            "N17", datetime(2026, 11, 21, 1, 0), "BATCH-N17", _rows("N17", 20, 4)))
        explained = dispatch.explain("N17")
        assert explained is assignment
        assert "WIN-NIGHT-1120" in explained.reason
        assert "最早关窗" in explained.reason

    def test_profile_rows_trace_back_to_manifest_source_batches(self):
        dispatch, profiler = _fresh()
        _register_overlapping_windows(dispatch)
        dispatch.on_arrival(PartitionArrival(
            "N16", datetime(2026, 11, 20, 23, 0), "BATCH-N16", _rows("N16", 10, 1)))
        dispatch.on_arrival(PartitionArrival(
            "N17", datetime(2026, 11, 21, 1, 0), "BATCH-N17", _rows("N17", 20, 4)))
        result = profiler.run_scheduled_window(dispatch, "WIN-NIGHT-1120", ASSET)

        # 画像行数与调度清单关联的来源批次一一对应
        stats = {ps["partition_id"]: ps for ps in result["partition_stats"]}
        assert stats["N17"]["batch_id"] == "BATCH-N17"
        assert stats["N17"]["row_count"] == 20
        assert stats["N17"]["null_count"] == 4
        assert stats["N17"]["null_ratio"] == 0.2
        manifest = dispatch.manifest("WIN-NIGHT-1120")
        assert manifest is not None
        assert {e.partition_id: e.batch_id for e in manifest.entries}["N17"] == "BATCH-N17"
        assert result["profile"]["来源批次"] == ["BATCH-N16", "BATCH-N17"]

        recon = profiler.reconcile_partition(dispatch, "N17")
        assert recon["contributions"][0]["batch_id"] == "BATCH-N17"
        assert recon["contributions"][0]["运行窗"] == "WIN-NIGHT-1120"


# ══════════════════════════════════════════════════════════════════
# 跨天分区在两次运行之间送达：原属窗已封闭，顺延且可解释
# ══════════════════════════════════════════════════════════════════

class TestLateArrivalBetweenRuns:

    def test_arrival_after_previous_window_sealed_spills_to_next(self):
        dispatch, profiler = _fresh()
        dispatch.register_window(
            "WIN-NIGHT-1120", "2026-11-20",
            datetime(2026, 11, 20, 18, 0), datetime(2026, 11, 21, 2, 0))
        dispatch.register_window(
            "WIN-DAY-1121", "2026-11-21",
            datetime(2026, 11, 21, 2, 0), datetime(2026, 11, 21, 18, 0))

        # 前晚作业 02:00 到点先跑（空清单）
        started = profiler.start_due_windows(dispatch, datetime(2026, 11, 21, 2, 0), ASSET)
        assert [r["window_id"] for r in started] == ["WIN-NIGHT-1120"]
        assert started[0]["profile"]["行数统计"] == "0"

        # 两次运行之间 N17 才送达：事件时间 01:30 原属前晚窗，但窗已封闭 → 顺延当日
        assignment = dispatch.on_arrival(PartitionArrival(
            "N17", datetime(2026, 11, 21, 1, 30), "BATCH-N17", _rows("N17", 20, 4)))
        assert assignment.window_id == "WIN-DAY-1121"
        assert "已封闭" in assignment.reason
        assert "顺延" in assignment.reason

        # 重放到达消息不改变归属
        replay = dispatch.on_arrival(PartitionArrival(
            "N17", datetime(2026, 11, 21, 1, 30), "BATCH-N17", _rows("N17", 20, 4)))
        assert replay.replayed is True
        assert replay.window_id == "WIN-DAY-1121"

        # 当日作业 18:00 启动，N17 计入当日；两份画像合计仍是 20 行 / 4 空
        started = profiler.start_due_windows(dispatch, datetime(2026, 11, 21, 18, 0), ASSET)
        assert [r["window_id"] for r in started] == ["WIN-DAY-1121"]
        recon = profiler.reconcile_partition(dispatch, "N17")
        assert (recon["total_rows"], recon["total_nulls"]) == (20, 4)
        assert recon["run_count"] == 1
        assert recon["balanced"] is True
        assert "N17" in profiler.daily_reconciliation("2026-11-21")["partitions"]
        assert "N17" not in profiler.daily_reconciliation("2026-11-20")["partitions"]


# ══════════════════════════════════════════════════════════════════
# 重复提交 / 重放 / 幂等运行
# ══════════════════════════════════════════════════════════════════

class TestDuplicateAndReplay:

    def test_same_partition_submitted_twice_counts_once(self):
        dispatch, profiler = _fresh()
        _register_overlapping_windows(dispatch)
        n17 = PartitionArrival("N17", datetime(2026, 11, 21, 1, 0), "BATCH-N17", _rows("N17", 20, 4))
        dispatch.on_arrival(n17)
        again = dispatch.on_arrival(n17)
        assert again.replayed is True
        entries = [e.partition_id for e in dispatch.manifest_entries("WIN-NIGHT-1120")]
        assert entries.count("N17") == 1
        result = profiler.run_scheduled_window(dispatch, "WIN-NIGHT-1120", ASSET)
        assert result["profile"]["行数统计"] == "20"

    def test_replay_with_different_batch_flagged_but_still_once(self):
        dispatch, profiler = _fresh()
        _register_overlapping_windows(dispatch)
        dispatch.on_arrival(PartitionArrival(
            "N17", datetime(2026, 11, 21, 1, 0), "BATCH-N17", _rows("N17", 20, 4)))
        replay = dispatch.on_arrival(PartitionArrival(
            "N17", datetime(2026, 11, 21, 1, 0), "BATCH-N17-RETRY", _rows("N17", 20, 4)))
        assert replay.replayed is True
        assert replay.batch_conflict is True
        assert replay.batch_id == "BATCH-N17"  # 来源批次以首次登记为准
        profiler.run_scheduled_window(dispatch, "WIN-NIGHT-1120", ASSET)
        recon = profiler.reconcile_partition(dispatch, "N17")
        assert recon["total_rows"] == 20
        assert recon["contributions"][0]["batch_id"] == "BATCH-N17"

    def test_window_run_is_idempotent(self):
        dispatch, profiler = _fresh()
        _register_overlapping_windows(dispatch)
        dispatch.on_arrival(PartitionArrival(
            "N17", datetime(2026, 11, 21, 1, 0), "BATCH-N17", _rows("N17", 20, 4)))
        first = profiler.run_scheduled_window(dispatch, "WIN-NIGHT-1120", ASSET)
        second = profiler.run_scheduled_window(dispatch, "WIN-NIGHT-1120", ASSET)
        assert second["reused"] is True
        assert second["profile"]["id"] == first["profile"]["id"]
        rows = [r for r in store_mod.store.rows("profile_stats")
                if r.get("运行窗") == "WIN-NIGHT-1120"]
        assert len(rows) == 1


# ══════════════════════════════════════════════════════════════════
# 依赖驱动启动与边界
# ══════════════════════════════════════════════════════════════════

class TestDependencyDrivenScheduling:

    def test_window_waits_for_upstream_dependencies(self):
        dispatch, _ = _fresh()
        _register_overlapping_windows(dispatch)
        # 只到 N16，N17 未达 → 前晚窗不就绪
        dispatch.on_arrival(PartitionArrival(
            "N16", datetime(2026, 11, 20, 23, 0), "BATCH-N16", _rows("N16", 10, 1)))
        assert dispatch.due_windows(datetime(2026, 11, 21, 0, 30)) == []
        assert dispatch.pending_expected("WIN-NIGHT-1120") == ["N17"]
        # N17 凌晨 1 点补到 → 立即就绪，不必等 02:00 关窗
        dispatch.on_arrival(PartitionArrival(
            "N17", datetime(2026, 11, 21, 1, 0), "BATCH-N17", _rows("N17", 20, 4)))
        assert dispatch.due_windows(datetime(2026, 11, 21, 1, 5)) == ["WIN-NIGHT-1120"]

    def test_arrival_outside_all_windows_raises(self):
        dispatch, _ = _fresh()
        _register_overlapping_windows(dispatch)
        with pytest.raises(NoOpenWindowError):
            dispatch.on_arrival(PartitionArrival(
                "X99", datetime(2026, 11, 22, 5, 0), "BATCH-X99", ()))

    def test_seal_unknown_window_raises(self):
        dispatch, profiler = _fresh()
        with pytest.raises(UnknownWindowError):
            profiler.run_scheduled_window(dispatch, "WIN-NOPE", ASSET)
