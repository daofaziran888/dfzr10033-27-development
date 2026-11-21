"""指标工厂领域服务：指标计算定义、聚合窗口、从立方到指标的产出管线。

指标发布不再直接读取整批 ``aggregate_cache.cached_data`` 自行分窗，而是消费
``sparse_window_metric`` 里已按事件时间切好、带观测状态的稀疏窗口结果，再把
发布结论回写到 ``window_confirmation`` 窗口确认清单。
"""
from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.sparse_window_metric import (
    SparseWindowMetricService,
    find_window,
    report_period,
)
from app.store import store


class MetricFactoryService(BaseModuleService):
    MODULE = "metric_factory"
    ENTITY = "计算指标"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["指标编号", "指标名称", "数据来源", "计算公式", "聚合窗口", "精度模式", "依赖立方", "最后产出"]
    REQUIRED_FIELDS = ["指标编号", "指标名称", "数据来源"]
    STATUS_ORDER = ["草稿", "投产中", "已产出", "产出异常"]
    ACTION_RULES = {"提交投产": "投产中", "确认产出": "已产出", "标记异常": "产出异常"}
    NEGATIVE_ACTIONS = ["标记异常"]

    def compute_metric(
        self,
        metric_id: int,
        cube_id: int,
        *,
        window_ref: Any = None,
        start: Any = None,
        end: Any = None,
        confirmed_through: Any = None,
    ) -> dict[str, Any]:
        """用稀疏分窗结果计算指标，并回写窗口确认清单。

        只有全部窗口都已观测时才产出整期值；存在空窗或缺证据窗口时，指标标记
        为 ``产出异常`` 且 ``value`` 为 ``None``，不会用 0 顶替缺证据。
        """
        metric = store.find(self.MODULE, metric_id)
        if metric is None:
            return {"ok": False, "message": "指标不存在"}

        cube = store.find("olap_cube", cube_id)
        if cube is None:
            return {"ok": False, "message": "依赖立方不存在"}

        # 窗口定义优先取显式参数，其次取指标登记的聚合窗口，最后回落到首个窗口定义。
        window = find_window(window_ref) if window_ref not in (None, "") else None
        if window is None:
            window = find_window(metric.get("聚合窗口"))
        if window is None:
            window = find_window(None)
        if window is None:
            return {"ok": False, "message": "未找到可用的窗口定义"}

        t0 = time.time()
        # 报表读取的是已落库的稀疏分窗行，不再自行读取整批 cached_data 分窗。
        report = report_period(
            cube_id=cube_id,
            window_ref=window.get("窗口别名"),
            metric_id=str(metric.get("指标编号", metric_id)),
            formula=metric.get("计算公式", "SUM"),
            start=start,
            end=end,
            confirmed_through=confirmed_through,
            refresh=True,
        )
        elapsed_ms = (time.time() - t0) * 1000
        if not report.get("ok"):
            return report

        complete = bool(report.get("完整"))
        metric_value = report.get("整期均值") if complete else None

        metric["最后产出"] = time.strftime("%Y-%m-%d %H:%M:%S")
        metric["status"] = "已产出" if complete else "产出异常"
        metric["pending"] = not complete
        metric["abnormal"] = not complete

        publication = SparseWindowMetricService().publish_report(
            report=report, window_ref=window.get("窗口别名"),
        )
        if not publication.get("ok"):
            return publication

        if complete:
            _cache_metric_result(metric_id, metric_value)
        _audit_metric(metric_id, elapsed_ms, report, complete)

        return {
            "ok": True,
            "metric_id": metric_id,
            "metric_no": metric.get("指标编号"),
            "cube_id": cube_id,
            "window": {
                "窗口编号": report.get("窗口编号"),
                "窗口别名": report.get("窗口别名"),
                "缺值策略": report.get("缺值策略"),
                "窗口范围": report.get("窗口范围"),
            },
            "value": metric_value,
            "published": complete,
            "report": report,
            "publication": publication.get("publication"),
            "确认清单": publication.get("确认清单", []),
            "elapsed_ms": elapsed_ms,
            "formula": metric.get("计算公式"),
            "缺证据按零处理": False,
        }


def _cache_metric_result(metric_id: int, value: float) -> None:
    rows = store.rows("result_cache")
    cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    cache = {
        "id": cache_id,
        "条目编号": f"RESU-M{metric_id:04d}",
        "查询摘要": f"metric_{metric_id}",
        "结果哈希": hashlib.md5(str(value).encode()).hexdigest()[:12],
        "缓存命中": "1",
        "过期时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 3600)),
        "存储大小": str(len(str(value))),
        "创建时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "最后命中": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "有效",
        "pending": True,
        "abnormal": False,
        "result_data": [{"metric_id": metric_id, "value": value}],
    }
    rows.append(cache)


def _audit_metric(
    metric_id: int, elapsed_ms: float, report: dict[str, Any], complete: bool
) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    value = report.get("整期均值")
    summary = (
        f"value={value:.2f}" if value is not None
        else f"整期未完成 空窗={report.get('空窗数')} 缺证据={report.get('缺证据数')}"
    )
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"METRIC_COMPUTE id={metric_id} {summary}",
        "执行耗时": f"{elapsed_ms:.0f}ms",
        "扫描行数": str(report.get("窗口数", 0)),
        "返回行数": str(report.get("已观测窗口数", 0)),
        "数据来源": "metric_factory",
        "操作用户": "metric_engine",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常" if complete else "慢查询",
        "pending": False,
        "abnormal": not complete,
    }
    rows.append(audit)
