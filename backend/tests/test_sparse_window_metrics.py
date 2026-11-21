"""稀疏分窗指标域集成测试：空窗/零值/缺证据、重叠窗口、幂等、补算与发布重试。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.metric_factory import MetricFactoryService
from app.services.sparse_window_metric import SparseWindowMetricService, parse_time
from app.store import store

client = TestClient(app)
AUX = ["window_event", "sparse_window_metric", "window_confirmation", "metric_publication"]


@pytest.fixture(autouse=True)
def clean_window_domain():
    # 某些用例会临时改写窗口定义，跑完必须还原，避免污染其他用例。
    window_snapshot = [dict(row) for row in store.rows("time_window")]
    metric_snapshot = [dict(row) for row in store.rows("metric_factory")]
    for table in AUX:
        store.rows(table).clear()
    yield
    for table in AUX:
        store.rows(table).clear()
    store.rows("time_window")[:] = [dict(row) for row in window_snapshot]
    store.rows("metric_factory")[:] = [dict(row) for row in metric_snapshot]


@pytest.fixture
def svc():
    return SparseWindowMetricService()


def _report(svc, **overrides):
    params = dict(
        cube_id="W1", window_ref="20m_window", metric_id="M1", formula="SUM",
        start="2026-11-01T00:00:00Z", end="2026-11-01T00:40:00Z",
        confirmed_through="2026-11-01T00:40:00Z",
    )
    params.update(overrides)
    return svc.report_period(**params)


def _window(svc, start, **overrides):
    rows = svc.list_metrics(cube_id=overrides.pop("cube_id", "W1"), window_ref="20m_window")
    matches = [row for row in rows if row["窗口起点"] == start]
    assert matches, f"未找到窗口 {start}"
    return matches[0]


class TestWindowComputation:
    def test_windows_slice_by_event_time_with_gap(self, svc):
        """相邻时段有观测，中间窗口仍保留为空窗而不是 0。"""
        svc.ingest_event("W1", "E1", "2026-11-01T00:05:00Z", 10)
        svc.ingest_event("W1", "E2", "2026-11-01T00:35:00Z", 7)
        report = _report(svc)

        middle = _window(svc, "2026-11-01T00:10:00Z")
        assert middle["观测状态"] == "空窗"
        assert middle["观测值"] is None
        assert middle["值类型"] == "无观测"
        assert middle["缺值原因"]
        assert report["空窗数"] == 1
        assert report["整期均值"] is None
        assert report["完整"] is False
        assert _window(svc, "2026-11-01T00:00:00Z")["观测值"] == 10.0
        assert _window(svc, "2026-11-01T00:20:00Z")["观测值"] == 7.0

    def test_window_outside_confirmed_range_is_missing_evidence(self, svc):
        """暗事件时间范围之外的窗口没有观测依据，归为缺证据。"""
        svc.ingest_event("W1", "E1", "2026-11-01T00:05:00Z", 10)
        report = _report(svc, start="2026-11-01T00:00:00Z", end="2026-11-01T00:40:00Z")
        late_window = _window(svc, "2026-11-01T00:30:00Z")
        assert late_window["观测状态"] == "缺证据"
        assert late_window["观测值"] is None
        assert "超出" in late_window["缺值原因"]
        assert report["缺证据数"] >= 1
        assert report["整期均值"] is None

    def test_overlapping_windows_share_event(self, svc):
        """宽度大于步长时同一事件可落入两个窗口，两个窗口各计一次。"""
        svc.ingest_event("W1", "E1", "2026-11-01T00:15:00Z", 4)
        _report(svc, end="2026-11-01T00:30:00Z")
        first = _window(svc, "2026-11-01T00:00:00Z")
        second = _window(svc, "2026-11-01T00:10:00Z")
        assert first["事件编号列表"] == ["E1"]
        assert second["事件编号列表"] == ["E1"]
        assert first["观测值"] == second["观测值"] == 4.0

    def test_duplicate_submission_counts_once(self, svc):
        """同一窗口重复提交同一事件编号只按事件身份算一次。"""
        svc.ingest_event("W1", "E1", "2026-11-01T00:05:00Z", 10)
        result = svc.ingest_event("W1", "E1", "2026-11-01T00:05:00Z", 10)
        assert result["duplicate"] is True
        _report(svc, end="2026-11-01T00:20:00Z")
        window = _window(svc, "2026-11-01T00:00:00Z")
        assert window["事件数"] == 1
        assert window["观测值"] == 10.0
        assert len(svc.list_events("W1")) == 1

    def test_true_zero_differs_from_missing(self, svc):
        """真实零值窗口记作实测零值，与空窗/缺证据严格区分。"""
        svc.ingest_event("W1", "E1", "2026-11-01T00:05:00Z", 0)
        _report(svc, end="2026-11-01T00:20:00Z")
        window = _window(svc, "2026-11-01T00:00:00Z")
        assert window["观测状态"] == "已观测"
        assert window["值类型"] == "实测零值"
        assert window["观测值"] == 0.0


class TestLateZero:
    def test_late_zero_feeds_both_overlapping_windows_and_period_mean(self, svc):
        """让同一零值样本迟到并落入两个重叠窗口，分别在到达前后读报表。"""
        svc.ingest_event("W1", "E1", "2026-11-01T00:05:00Z", 10)
        svc.ingest_event("W1", "E2", "2026-11-01T00:35:00Z", 7)
        before = _report(svc)
        assert before["已观测均值"] == 8.0
        assert before["整期均值"] is None

        arrival = svc.ingest_event("W1", "E3", "2026-11-01T00:15:00Z", 0)
        assert arrival["late"] is True
        affected_starts = {item["窗口起点"] for item in arrival["affected_windows"]}
        assert affected_starts == {"2026-11-01T00:00:00Z", "2026-11-01T00:10:00Z"}

        after = _report(svc, refresh=False)
        assert after["空窗数"] == 0
        assert after["实测零值窗口数"] == 1
        assert after["完整"] is True
        # 区间内起点落在 [00:00, 00:40) 的 4 个窗口：10 / 0 / 7 / 7。
        assert after["整期均值"] == pytest.approx(6.0)
        # 零值被两个重叠窗口各计一次，但事件身份只有一个。
        assert after["去重事件数"] == 3
        # 事件位次：E1、E3 各落 2 个重叠窗口，E2 落 2 个，共 5 个位次。
        assert after["窗口事件位次数"] == 5
        assert after["重叠计入次数"] == 2

    def test_late_event_zero_cannot_be_mistaken_for_missing(self, svc):
        """补入零值后，空窗转为实测零值；缺证据窗口仍保持 None。"""
        svc.ingest_event("W1", "E1", "2026-11-01T00:05:00Z", 10)
        svc.ingest_event("W1", "E2", "2026-11-01T00:35:00Z", 7)
        _report(svc, start="2026-11-01T00:00:00Z", end="2026-11-01T01:00:00Z")
        assert _window(svc, "2026-11-01T00:40:00Z")["观测状态"] == "缺证据"
        svc.ingest_event("W1", "E3", "2026-11-01T00:15:00Z", 0)
        svc.recompute_window(_window(svc, "2026-11-01T00:10:00Z")["id"])
        report = _report(svc, start="2026-11-01T00:00:00Z", end="2026-11-01T01:00:00Z", refresh=False)
        assert report["缺证据数"] >= 1
        assert report["整期均值"] is None


class TestRecompute:
    def test_recompute_uses_snapshotted_alignment(self, svc):
        """补算按当时确认的事件时间与对齐基准，配置改动不改变既有边界。"""
        svc.ingest_event("W1", "E1", "2026-11-01T00:05:00Z", 10)
        svc.ingest_event("W1", "E2", "2026-11-01T00:35:00Z", 7)
        _report(svc, end="2026-11-01T00:40:00Z")
        window = _window(svc, "2026-11-01T00:10:00Z")
        snapshot = (window["窗口起点"], window["窗口终点"], window["对齐基准"], window["事件时间基准"])
        assert window["观测状态"] == "空窗"

        # 事后改窗口配置
        definition = next(row for row in store.rows("time_window") if row["窗口别名"] == "20m_window")
        definition["对齐基准"] = "epoch"
        definition["滑动宽度"] = "3600"

        svc.ingest_event("W1", "E3", "2026-11-01T00:15:00Z", 0)
        result = svc.recompute_window(window["id"])
        recomputed = result["metric"]
        assert (recomputed["窗口起点"], recomputed["窗口终点"]) == snapshot[:2]
        assert recomputed["对齐基准"] == snapshot[2]
        assert recomputed["事件时间基准"] == snapshot[3]
        assert recomputed["观测状态"] == "已观测"
        assert recomputed["观测值"] == 0.0

    def test_events_after_confirmed_base_are_ignored(self, svc):
        """补算只用确认时点之前的事件，迟于基准的提交不进入窗口。"""
        svc.ingest_event("W1", "E1", "2026-11-01T00:05:00Z", 10)
        _report(svc, end="2026-11-01T00:20:00Z", confirmed_through="2026-11-01T00:20:00Z")
        svc.ingest_event("W1", "E9", "2026-11-01T00:25:00Z", 99)
        window = _window(svc, "2026-11-01T00:00:00Z")
        svc.recompute_window(window["id"])
        assert window["事件时间基准"] == "2026-11-01T00:20:00Z"
        assert window["观测值"] == 10.0


class TestPublication:
    def _seed_gap(self, svc):
        svc.ingest_event("W1", "E1", "2026-11-01T00:05:00Z", 10)
        svc.ingest_event("W1", "E2", "2026-11-01T00:35:00Z", 7)
        return svc.publish_period(
            metric_id="M1", cube_id="W1", window_ref="20m_window", formula="SUM",
            start="2026-11-01T00:00:00Z", end="2026-11-01T00:40:00Z",
            confirmed_through="2026-11-01T00:40:00Z",
        )

    def test_publication_interrupts_and_writes_confirmations(self, svc):
        """指标工厂准备发布整期数据时中间窗还是空的，发布中断并回写确认清单。"""
        published = self._seed_gap(svc)
        publication = published["publication"]
        assert publication["状态"] == "发布中断"
        assert publication["仍缺观测窗口数"] == 1
        assert publication["分母事件数"] == 2
        assert publication["整期均值"] is None
        results = [item["确认结果"] for item in published["确认清单"]]
        assert "仍缺观测" in results
        assert "已发布" in results
        assert all(item["确认结果"] != "实测为零" for item in published["确认清单"])

    def test_retry_recomputes_denominator_by_event_identity(self, svc):
        """发布中断后重试，按事件编号复算分母并区分三种确认结果。"""
        published = self._seed_gap(svc)
        publication_no = published["publication"]["发布编号"]
        # 既有事件重复提交 + 迟到零值入窗
        svc.ingest_event("W1", "E1", "2026-11-01T00:05:00Z", 10)
        svc.ingest_event("W1", "E3", "2026-11-01T00:15:00Z", 0)

        retry = svc.retry_publication(publication_no)
        assert retry["ok"] is True
        assert retry["分母事件数"] == 3
        assert retry["publication"]["提交记录数"] == 4  # E1 提交两次
        assert retry["确认结果统计"] == {"已发布": 3, "实测为零": 1, "仍缺观测": 0}
        assert retry["publication"]["状态"] == "已重试完成"
        assert retry["publication"]["整期均值"] == pytest.approx(6.0)

        confirmations = svc.list_confirmations(publication_no=publication_no)
        zero_rows = [row for row in confirmations if row["确认结果"] == "实测为零"]
        assert zero_rows and zero_rows[0]["观测值"] == 0.0
        assert zero_rows[0]["值类型"] == "实测零值"

    def test_publication_links_back_to_window_events(self, svc):
        """发布记录可按事件编号关联回入窗事件。"""
        published = self._seed_gap(svc)
        linked = svc.publication_events(published["publication"]["发布编号"])
        assert linked["ok"] is True
        assert linked["分母事件数"] == 2
        event_nos = {item["事件编号"] for item in linked["events"]}
        assert event_nos == {"E1", "E2"}
        # E2 落在两个重叠窗口里，E1 只落在一个窗口里
        assert len(linked["events"]) == 3


class TestMetricFactoryIntegration:
    def test_factory_consumes_sparse_windows_not_raw_batches(self, svc):
        """指标工厂读稀疏分窗结果产出整期数据，并回写窗口确认清单。"""
        svc.ingest_event("2", "E1", "2026-11-01T00:05:00Z", 10)
        svc.ingest_event("2", "E2", "2026-11-01T00:35:00Z", 7)
        result = MetricFactoryService().compute_metric(2, 2)
        assert result["ok"] is True
        assert result["window"]["窗口别名"] == "20m_window"
        assert result["published"] is False
        assert result["value"] is None
        assert store.find("metric_factory", 2)["status"] == "产出异常"
        assert result["确认清单"]
        assert {item["确认结果"] for item in result["确认清单"]} == {"仍缺观测", "已发布"}

    def test_factory_publishes_complete_period(self, svc):
        svc.ingest_event("2", "E1", "2026-11-01T00:05:00Z", 10)
        svc.ingest_event("2", "E2", "2026-11-01T00:15:00Z", 2)
        result = MetricFactoryService().compute_metric(2, 2)
        assert result["published"] is True
        # 两个重叠窗口：00:00-00:20 收 10+2，00:10-00:30 收 2，整期均值 7。
        assert result["value"] == pytest.approx(7.0)
        assert store.find("metric_factory", 2)["status"] == "已产出"
        assert result["publication"]["状态"] == "已发布"


class TestWindowMetricsApi:
    def test_end_to_end_api_flow(self):
        created = client.post("/api/window-metrics/events", params={
            "cube_id": "API1", "event_no": "A1",
            "event_time": "2026-11-01T00:05:00Z", "value": 10})
        assert created.status_code == 200

        computed = client.post("/api/window-metrics/compute", params={
            "cube_id": "API1", "window_ref": "20m_window", "metric_id": "M1",
            "start": "2026-11-01T00:00:00Z", "end": "2026-11-01T00:40:00Z",
            "confirmed_through": "2026-11-01T00:40:00Z"})
        assert computed.status_code == 200
        payload = computed.json()
        assert payload["summary"]["空窗数"] == 0
        assert payload["summary"]["缺证据数"] >= 1

        report = client.get("/api/window-metrics/report", params={
            "cube_id": "API1", "window_ref": "20m_window", "metric_id": "M1",
            "start": "2026-11-01T00:00:00Z", "end": "2026-11-01T00:40:00Z",
            "confirmed_through": "2026-11-01T00:40:00Z", "refresh": False})
        assert report.status_code == 200
        assert report.json()["整期均值"] is None

        listed = client.get("/api/window-metrics", params={"cube_id": "API1"})
        assert listed.status_code == 200 and listed.json()["total"] >= 1

    def test_aux_tables_stay_out_of_module_overview(self):
        modules = store.module_names()
        assert "window_event" not in modules
        assert "sparse_window_metric" not in modules
        overview = client.get("/api/overview").json()
        names = {row["name"] for row in overview["modules"]}
        assert "sparse_window_metric" not in names
        assert "time_window" in names

    def test_invalid_event_time_rejected(self):
        response = client.post("/api/window-metrics/events", params={
            "cube_id": "API1", "event_no": "BAD",
            "event_time": "not-a-time", "value": 1})
        assert response.status_code == 400


def test_parse_time_handles_iso_and_epoch():
    assert parse_time("2026-11-01T00:00:00Z").timestamp() == 1793491200.0
    assert parse_time(1793491200).timestamp() == 1793491200.0


class TestBusinessScenario:
    def test_late_zero_between_report_reads_and_retry(self, svc):
        """业务报表取已观测时段，整期均值取决于空窗是否收到迟到的真实零值。"""
        svc.ingest_event("B1", "E1", "2026-11-01T00:05:00Z", 10)
        svc.ingest_event("B1", "E2", "2026-11-01T00:35:00Z", 7)
        svc.compute_windows(
            cube_id="B1", window_ref="20m_window", metric_id="BIZ", formula="SUM",
            start="2026-11-01T00:00:00Z", end="2026-11-01T00:40:00Z",
            confirmed_through="2026-11-01T00:40:00Z",
        )
        published = svc.publish_period(
            metric_id="BIZ", cube_id="B1", window_ref="20m_window", formula="SUM",
            start="2026-11-01T00:00:00Z", end="2026-11-01T00:40:00Z",
            confirmed_through="2026-11-01T00:40:00Z",
        )
        assert published["publication"]["状态"] == "发布中断"

        before = svc.report_period(
            cube_id="B1", window_ref="20m_window", metric_id="BIZ", formula="SUM",
            start="2026-11-01T00:00:00Z", end="2026-11-01T00:40:00Z", refresh=False,
        )
        assert before["已观测均值"] == 8.0
        assert before["整期均值"] is None

        arrival = svc.ingest_event("B1", "E3", "2026-11-01T00:15:00Z", 0)
        assert arrival["late"] is True
        assert len(arrival["affected_windows"]) == 2

        after = svc.report_period(
            cube_id="B1", window_ref="20m_window", metric_id="BIZ", formula="SUM",
            start="2026-11-01T00:00:00Z", end="2026-11-01T00:40:00Z", refresh=False,
        )
        assert after["完整"] is True
        assert after["整期均值"] == pytest.approx(6.0)

        retry = svc.retry_publication(published["publication"]["发布编号"])
        assert retry["确认结果统计"] == {"已发布": 3, "实测为零": 1, "仍缺观测": 0}
        assert retry["分母事件数"] == 3
        assert retry["publication"]["提交记录数"] == 3
