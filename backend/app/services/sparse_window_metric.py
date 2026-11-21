"""稀疏分窗指标领域服务：从带事件时间的立方聚合明细产出可审计的窗口指标。

设计要点（对应业务约束）：

* 窗口按数据源的事件时间切片，滑动窗口宽大于步长时同一事件可落入多个窗口；
  同一窗口内重复提交的同一事件编号只计一次。
* 每个窗口明确区分三种结果：``已观测``（含 ``实测值`` / ``实测零值``）、
  ``空窗``（相邻时段已有观测，但本窗口没有事件）和 ``缺证据``（超出已确认的
  事件时间范围，根本没有观测依据）。后两者 ``观测值`` 恒为 ``None``，缺值原因
  随窗口保存，任何统计都不会把它们折算成 0。
* 窗口行快照保存 ``对齐基准``、``滑动宽度``、``滑动步长``、``缺值策略`` 以及
  ``事件时间基准``。补算只使用这些快照，配置后来改动不会改变既有窗口边界。
* 整期报表把 ``已观测均值`` 与 ``整期均值`` 分开：只要还有空窗/缺证据窗口，
  ``整期均值`` 就是 ``None``；迟到的真实零值落入空窗后，该窗口转为 ``实测零值``，
  整期均值才会把这些 0 计入。
* 发布记录按事件编号复算分母；中断后重试使用同一份窗口快照，回写窗口确认清单，
  并区分 ``已发布`` / ``实测为零`` / ``仍缺观测``。
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from app.store import store

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
SHARED_METRIC = ""
MAX_WINDOWS = 5000


# ---------- 时间与对齐 ----------

def parse_time(value: Any) -> datetime:
    """把 epoch 秒、ISO8601（含 Z / 偏移）统一转成 UTC 时间。"""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    text = str(value).strip()
    if not text:
        raise ValueError("事件时间不能为空")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_time(value: datetime | float | int) -> str:
    if isinstance(value, datetime):
        moment = value
    else:
        moment = datetime.fromtimestamp(float(value), tz=timezone.utc)
    return moment.astimezone(timezone.utc).strftime(ISO_FORMAT)


def _seconds(value: Any) -> float:
    return parse_time(value).timestamp()


def _floor_to(ts: float, step: int, origin: float = 0.0) -> float:
    return origin + ((ts - origin) // step) * step


def _ceil_to(ts: float, step: int, origin: float = 0.0) -> float:
    floored = _floor_to(ts, step, origin)
    return floored if floored == ts else floored + step


def _alignment_base(window: dict[str, Any]) -> tuple[float, str]:
    """返回（对齐原点秒，对齐基准标识）。

    ``epoch`` 以 Unix 纪元为原点；其余值视为 ISO 时间的对齐原点。这样补算时
    只需读取窗口行里的快照即可复原同样的窗口边界。
    """
    raw = str(window.get("对齐基准") or "epoch").strip()
    if raw.lower() in {"", "epoch", "unix", "0"}:
        return 0.0, "epoch"
    return _seconds(raw), raw


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


# ---------- 数据读取 ----------

def _windows() -> list[dict[str, Any]]:
    return store.rows("time_window")


def find_window(ref: Any) -> dict[str, Any] | None:
    """按 id / 窗口编号 / 窗口别名定位窗口定义；空值取第一条，找不到返回 None。

    已经解析出的窗口行也可以直接传回来，方便内部各步骤透传同一份快照。
    """
    if isinstance(ref, dict):
        return ref
    rows = _windows()
    if ref is None or str(ref).strip() == "":
        return rows[0] if rows else None
    target = str(ref).strip()
    for row in rows:
        if target in {str(row.get("id", "")), str(row.get("窗口编号", "")), str(row.get("窗口别名", ""))}:
            return row
    return None


def _resolve_window(ref: Any) -> dict[str, Any] | None:
    """计算入口的宽松解析：无法匹配任何窗口时回落到第一条定义。"""
    return find_window(ref) or (find_window(None) if ref not in (None, "") else None)


def _events(cube_id: Any) -> list[dict[str, Any]]:
    rows = [r for r in store.rows("window_event") if str(r.get("立方编号")) == str(cube_id)]
    return sorted(rows, key=lambda row: float(row.get("事件时间秒", 0)))


def _metric_rows(
    *,
    cube_id: Any,
    window: dict[str, Any] | None,
    metric_id: str,
    start_ts: float | None = None,
    end_ts: float | None = None,
) -> list[dict[str, Any]]:
    """按窗口起点落在 [start_ts, end_ts) 读取窗口行。

    用起点筛选而不是用整窗包含关系，重叠窗口的尾窗才不会被范围截掉。
    """
    rows = [
        row for row in store.rows("sparse_window_metric")
        if str(row.get("立方编号")) == str(cube_id)
        and str(row.get("指标编号", "")) == metric_id
        and (window is None or str(row.get("窗口定义", "")) == str(window.get("窗口编号", "")))
    ]
    if start_ts is not None:
        rows = [row for row in rows if float(row.get("窗口起点秒", 0)) >= start_ts]
    if end_ts is not None:
        rows = [row for row in rows if float(row.get("窗口起点秒", 0)) < end_ts]
    return sorted(rows, key=lambda row: float(row.get("窗口起点秒", 0)))


def _aggregate(formula: str, events: list[dict[str, Any]]) -> tuple[float | None, int, list[str]]:
    """按事件编号去重后聚合，返回（值、事件数、事件编号列表）。"""
    grouped: dict[str, float] = {}
    for event in events:
        event_no = str(event.get("事件编号", "")).strip()
        if not event_no:
            continue
        grouped.setdefault(event_no, float(event.get("观测值", 0)))
    values = list(grouped.values())
    event_nos = sorted(grouped)
    if not values:
        return None, 0, []

    upper = formula.upper()
    if "COUNT" in upper:
        result = float(len(values))
    elif "AVG" in upper:
        result = sum(values) / len(values)
    elif "MAX" in upper:
        result = max(values)
    elif "MIN" in upper:
        result = min(values)
    else:
        result = sum(values)
    return float(result), len(values), event_nos


def _window_range(
    window: dict[str, Any],
    start: Any,
    end: Any,
    events: list[dict[str, Any]],
) -> tuple[float, float] | tuple[None, str]:
    """把请求的区间对齐到窗口步长，返回 [start_ts, end_ts) 的窗口起点范围。"""
    origin, _ = _alignment_base(window)
    step = int(float(window.get("滑动步长") or window.get("滑动宽度") or 3600))
    if step <= 0:
        raise ValueError("滑动步长必须为正数")
    if start in (None, ""):
        if not events:
            return None, "没有可计算的事件且未指定窗口范围"
        start_ts = _floor_to(float(events[0]["事件时间秒"]), step, origin)
    else:
        start_ts = _floor_to(_seconds(start), step, origin)
    if end in (None, ""):
        if not events:
            return None, "没有可计算的事件且未指定窗口范围"
        end_ts = _ceil_to(float(events[-1]["事件时间秒"]), step, origin)
    else:
        end_ts = _ceil_to(_seconds(end), step, origin)
    if end_ts <= start_ts:
        return None, "窗口终点必须晚于起点"
    return start_ts, end_ts


def _window_events(
    events: list[dict[str, Any]], start_ts: float, end_ts: float, base_ts: float | None
) -> list[dict[str, Any]]:
    selected = []
    for event in events:
        event_ts = float(event.get("事件时间秒", 0))
        if start_ts <= event_ts < end_ts and (base_ts is None or event_ts <= base_ts):
            selected.append(event)
    return selected


def _state_for_empty(
    start_ts: float,
    end_ts: float,
    first_ts: float | None,
    last_ts: float | None,
) -> tuple[str, str]:
    """没有事件时判定是“空窗”还是“缺证据”，并给出可追溯的原因。

    * 空窗：窗口夹在两次已观测事件之间（前有事件、后有事件），只是本窗口没收到
      事件，因此不能判定为真实零值。
    * 缺证据：窗口在首个事件之前或最后一个事件之后，或根本没有事件。此时没有
      任何观测依据，既不能记零，也不能算作“已确认为空”。
    """
    if first_ts is None or last_ts is None:
        return "缺证据", "未收到任何事件，无法确认该时段的观测"
    if first_ts < start_ts and last_ts >= end_ts:
        return "空窗", "相邻时段已有观测，但本窗口未收到事件，不能判定为真实零值"
    if end_ts <= first_ts:
        return "缺证据", "窗口早于首个已确认事件，缺少观测证据"
    return "缺证据", "窗口超出已确认事件时间范围，缺少观测证据"


# ---------- 服务 ----------

class SparseWindowMetricService:
    """稀疏分窗指标：事件登记、窗口计算、报表读取、整期发布与确认回写。"""

    # -- 事件 --

    def ingest_event(
        self,
        cube_id: Any,
        event_no: str,
        event_time: Any,
        value: Any,
        *,
        batch: str = "manual",
    ) -> dict[str, Any]:
        """登记一条带事件时间的聚合明细。

        事件编号在立方内唯一：重复提交只累加提交次数，不产生第二条事件。
        事件登记后会自动补算已存在的、覆盖该事件时间的窗口行。
        """
        event_no = str(event_no or "").strip()
        if not event_no:
            return {"ok": False, "message": "事件编号不能为空"}
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return {"ok": False, "message": "观测值必须是数值"}
        try:
            moment = parse_time(event_time)
        except ValueError as exc:
            return {"ok": False, "message": f"事件时间不合法：{exc}"}

        rows = store.rows("window_event")
        existing = next(
            (row for row in rows
             if str(row.get("立方编号")) == str(cube_id) and str(row.get("事件编号")) == event_no),
            None,
        )
        watermark_before = max(
            (float(row.get("事件时间秒", 0)) for row in rows
             if str(row.get("立方编号")) == str(cube_id)),
            default=None,
        )
        new_arrival = _now_text()
        # 迟到判定：事件时间早于已到达的事件时间水位。首个事件不算迟到。
        late = watermark_before is not None and moment.timestamp() < watermark_before

        if existing is not None:
            existing["重复提交次数"] = int(existing.get("重复提交次数", 0)) + 1
            existing["最后提交"] = new_arrival
            if late:
                existing["迟到"] = True
            event = existing
            duplicate = True
        else:
            event = {
                "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
                "事件编号": event_no,
                "立方编号": str(cube_id),
                "事件时间": format_time(moment),
                "事件时间秒": moment.timestamp(),
                "观测值": numeric,
                "提交批次": batch,
                "到达时间": new_arrival,
                "重复提交次数": 0,
                "迟到": late,
                "status": "已登记",
                "pending": False,
                "abnormal": False,
            }
            rows.append(event)
            duplicate = False

        affected = self._refresh_windows_for_event(str(cube_id), moment.timestamp())
        return {
            "ok": True,
            "duplicate": duplicate,
            "late": late,
            "event": event,
            "affected_windows": affected,
        }

    def list_events(self, cube_id: Any | None = None) -> list[dict[str, Any]]:
        rows = _events(cube_id) if cube_id is not None else list(store.rows("window_event"))
        return sorted(rows, key=lambda row: (str(row.get("立方编号")), float(row.get("事件时间秒", 0))))

    def _refresh_windows_for_event(self, cube_id: str, event_ts: float) -> list[dict[str, Any]]:
        affected: list[dict[str, Any]] = []
        for row in list(store.rows("sparse_window_metric")):
            if str(row.get("立方编号")) != cube_id:
                continue
            window_start = float(row.get("窗口起点秒", 0))
            window_end = float(row.get("窗口终点秒", 0))
            base = row.get("事件时间基准")
            base_ts = _seconds(base) if base else None
            if window_start <= event_ts < window_end and (base_ts is None or event_ts <= base_ts):
                outcome = self.recompute_window(int(row["id"]))
                if outcome.get("ok"):
                    affected.append({
                        "窗口起点": row.get("窗口起点"),
                        "观测状态": row.get("观测状态"),
                        "观测值": row.get("观测值"),
                    })
        return affected

    # -- 计算 --

    def compute_windows(
        self,
        *,
        cube_id: Any,
        window_ref: Any = None,
        metric_id: str = SHARED_METRIC,
        formula: str = "SUM",
        start: Any = None,
        end: Any = None,
        confirmed_through: Any = None,
        persist: bool = True,
    ) -> dict[str, Any]:
        """按事件时间把立方明细切成稀疏窗口，写入/更新 ``sparse_window_metric``。"""
        window = _resolve_window(window_ref)
        if window is None:
            return {"ok": False, "message": "窗口定义不存在"}
        try:
            width = int(float(window.get("滑动宽度") or 3600))
            step = int(float(window.get("滑动步长") or width))
        except (TypeError, ValueError):
            return {"ok": False, "message": "窗口宽度与步长必须为数值"}
        if width <= 0 or step <= 0:
            return {"ok": False, "message": "窗口宽度与步长必须为正数"}

        origin, alignment_label = _alignment_base(window)

        events = _events(cube_id)
        try:
            base_ts = _seconds(confirmed_through) if confirmed_through not in (None, "") else (
                float(events[-1]["事件时间秒"]) if events else None
            )
        except ValueError as exc:
            return {"ok": False, "message": f"确认事件时间不合法：{exc}"}

        events = [event for event in events
                  if base_ts is None or float(event.get("事件时间秒", 0)) <= base_ts]

        try:
            bounds = _window_range(window, start, end, events)
        except ValueError as exc:
            return {"ok": False, "message": f"窗口范围不合法：{exc}"}
        if bounds[0] is None:
            return {"ok": False, "message": bounds[1]}
        start_ts, end_ts = bounds

        count = int((end_ts - start_ts) // step)
        if count > MAX_WINDOWS:
            return {"ok": False, "message": f"窗口数量 {count} 超过上限 {MAX_WINDOWS}"}

        first_ts = float(events[0]["事件时间秒"]) if events else None
        last_ts = float(events[-1]["事件时间秒"]) if events else None
        base_text = format_time(base_ts) if base_ts is not None else None

        produced: list[dict[str, Any]] = []
        cursor = start_ts
        while cursor < end_ts:
            window_end = cursor + width
            window_events = _window_events(events, cursor, window_end, base_ts)
            produced.append(self._materialize(
                cube_id=cube_id,
                window=window,
                metric_id=metric_id,
                formula=formula,
                start_ts=cursor,
                end_ts=window_end,
                events=window_events,
                first_ts=first_ts,
                last_ts=last_ts,
                width=width,
                step=step,
                alignment_label=alignment_label,
                base_text=base_text,
                persist=persist,
            ))
            cursor += step

        return {
            "ok": True,
            "cube_id": str(cube_id),
            "window_id": window.get("窗口编号"),
            "window_alias": window.get("窗口别名"),
            "metric_key": metric_id,
            "alignment": alignment_label,
            "event_time_base": base_text,
            "window_count": len(produced),
            "windows": produced,
            "summary": self._summarize(produced),
        }

    def _materialize(
        self,
        *,
        cube_id: Any,
        window: dict[str, Any],
        metric_id: str,
        formula: str,
        start_ts: float,
        end_ts: float,
        events: list[dict[str, Any]],
        first_ts: float | None,
        last_ts: float | None,
        width: int,
        step: int,
        alignment_label: str,
        base_text: str | None,
        persist: bool,
    ) -> dict[str, Any]:
        value, event_count, event_nos = _aggregate(formula, events)
        if event_count:
            observed_state = "已观测"
            value_kind = "实测零值" if value == 0 else "实测值"
            missing_reason = None
        else:
            observed_state, missing_reason = _state_for_empty(start_ts, end_ts, first_ts, last_ts)
            value_kind = "无观测"

        row = self._find_row(cube_id, window, metric_id, start_ts)
        if row is None:
            row = {
                "id": max((int(r.get("id", 0)) for r in store.rows("sparse_window_metric")), default=0) + 1,
                "指标编号": metric_id,
                "立方编号": str(cube_id),
                "窗口定义": str(window.get("窗口编号", "")),
                "窗口编号": window.get("窗口编号"),
                "窗口别名": window.get("窗口别名"),
                "窗口起点秒": start_ts,
                "窗口终点秒": end_ts,
                "重算次数": 0,
                "status": observed_state,
                "pending": observed_state != "已观测",
                "abnormal": observed_state == "缺证据",
            }
            if persist:
                store.rows("sparse_window_metric").append(row)
        else:
            row["重算次数"] = int(row.get("重算次数", 0)) + 1

        row.update({
            "窗口起点": format_time(start_ts),
            "窗口终点": format_time(end_ts),
            "观测状态": observed_state,
            "值类型": value_kind,
            "观测值": value,
            "事件数": event_count,
            "事件编号列表": event_nos,
            "缺值原因": missing_reason,
            "缺值策略": window.get("缺值策略", "keep_null"),
            "对齐基准": alignment_label,
            "滑动宽度": width,
            "滑动步长": step,
            "事件时间基准": base_text,
            "公式": formula,
            "计算时间": _now_text(),
            "status": observed_state,
            "pending": observed_state != "已观测",
            "abnormal": observed_state == "缺证据",
        })
        return row

    def _find_row(
        self, cube_id: Any, window: dict[str, Any], metric_id: str, start_ts: float
    ) -> dict[str, Any] | None:
        for row in store.rows("sparse_window_metric"):
            if (
                str(row.get("立方编号")) == str(cube_id)
                and str(row.get("指标编号", "")) == metric_id
                and str(row.get("窗口定义", "")) == str(window.get("窗口编号", ""))
                and float(row.get("窗口起点秒", -1)) == start_ts
            ):
                return row
        return None

    def _summarize(self, rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
        rows = list(rows)
        observed = [row for row in rows if row.get("观测状态") == "已观测"]
        zeros = [row for row in observed if row.get("值类型") == "实测零值"]
        empty = [row for row in rows if row.get("观测状态") == "空窗"]
        missing = [row for row in rows if row.get("观测状态") == "缺证据"]
        return {
            "窗口数": len(rows),
            "已观测窗口数": len(observed),
            "空窗数": len(empty),
            "缺证据数": len(missing),
            "实测零值窗口数": len(zeros),
            "事件数": sum(int(row.get("事件数", 0)) for row in rows),
            "已观测均值": self._mean([float(row["观测值"]) for row in observed]),
            "缺证据按零处理": False,
        }

    @staticmethod
    def _mean(values: list[float]) -> float | None:
        if not values:
            return None
        return round(sum(values) / len(values), 6)

    # -- 补算 --

    def recompute_window(self, metric_row_id: int) -> dict[str, Any]:
        """按窗口行快照补算单个窗口。

        只使用行里保存的 ``对齐基准`` / ``滑动宽度`` / ``事件时间基准``，不读取
        当前窗口定义。这样即使事后调整了窗口配置，已确认的窗口边界也不会漂移。
        """
        row = store.find("sparse_window_metric", metric_row_id)
        if row is None:
            return {"ok": False, "message": "窗口指标不存在"}

        start_ts = float(row["窗口起点秒"])
        width = int(row["滑动宽度"])
        end_ts = start_ts + width
        base_text = row.get("事件时间基准")
        base_ts = _seconds(base_text) if base_text else None

        events = [event for event in _events(row.get("立方编号"))
                  if base_ts is None or float(event.get("事件时间秒", 0)) <= base_ts]
        first_ts = float(events[0]["事件时间秒"]) if events else None
        last_ts = float(events[-1]["事件时间秒"]) if events else None

        window_events = _window_events(events, start_ts, end_ts, base_ts)
        value, event_count, event_nos = _aggregate(str(row.get("公式", "SUM")), window_events)
        if event_count:
            observed_state = "已观测"
            value_kind = "实测零值" if value == 0 else "实测值"
            missing_reason = None
        else:
            observed_state, missing_reason = _state_for_empty(start_ts, end_ts, first_ts, last_ts)
            value_kind = "无观测"

        row.update({
            "窗口终点": format_time(end_ts),
            "窗口终点秒": end_ts,
            "观测状态": observed_state,
            "值类型": value_kind,
            "观测值": value,
            "事件数": event_count,
            "事件编号列表": event_nos,
            "缺值原因": missing_reason,
            "重算次数": int(row.get("重算次数", 0)) + 1,
            "计算时间": _now_text(),
            "status": observed_state,
            "pending": observed_state != "已观测",
            "abnormal": observed_state == "缺证据",
        })
        return {"ok": True, "metric": row}

    # -- 报表 --

    def report_period(
        self,
        *,
        cube_id: Any,
        window_ref: Any = None,
        metric_id: str = SHARED_METRIC,
        formula: str = "SUM",
        start: Any = None,
        end: Any = None,
        confirmed_through: Any = None,
        refresh: bool = True,
    ) -> dict[str, Any]:
        """业务报表：只把已观测窗口计入统计，并明确整期是否完整。"""
        window = _resolve_window(window_ref)
        if window is None:
            return {"ok": False, "message": "窗口定义不存在"}

        if refresh:
            computed = self.compute_windows(
                cube_id=cube_id, window_ref=window, metric_id=metric_id,
                formula=formula, start=start, end=end, confirmed_through=confirmed_through,
            )
            if not computed.get("ok"):
                return computed
            if not computed["windows"]:
                return {"ok": False, "message": "窗口范围内没有可统计的窗口"}
            start_ts = float(computed["windows"][0]["窗口起点秒"])
            end_ts = float(computed["windows"][-1]["窗口起点秒"]) + int(window.get("滑动步长") or 1)
        else:
            try:
                bounds = _window_range(window, start, end, _events(cube_id))
            except ValueError:
                return {"ok": False, "message": "窗口范围不合法"}
            if bounds[0] is None:
                return {"ok": False, "message": bounds[1]}
            start_ts, end_ts = bounds

        rows = _metric_rows(
            cube_id=cube_id, window=window, metric_id=metric_id,
            start_ts=start_ts, end_ts=end_ts,
        )
        observed = [row for row in rows if row.get("观测状态") == "已观测"]
        zeros = [row for row in observed if row.get("值类型") == "实测零值"]
        empty = [row for row in rows if row.get("观测状态") == "空窗"]
        missing = [row for row in rows if row.get("观测状态") == "缺证据"]
        observed_values = [float(row["观测值"]) for row in observed]
        complete = bool(rows) and not empty and not missing

        distinct_events = {
            event_no
            for row in rows
            for event_no in row.get("事件编号列表", [])
        }
        event_slots = sum(int(row.get("事件数", 0)) for row in rows)

        return {
            "ok": True,
            "指标编号": metric_id,
            "立方编号": str(cube_id),
            "窗口编号": window.get("窗口编号"),
            "窗口别名": window.get("窗口别名"),
            "缺值策略": window.get("缺值策略"),
            "窗口范围": {
                # 起点用窗口起点去重，终点取所有窗口终点的最大值，重叠尾窗不会被截断。
                "start": rows[0].get("窗口起点") if rows else None,
                "end": (
                    format_time(max(float(row["窗口终点秒"]) for row in rows))
                    if rows else None
                ),
                "start_ts": start_ts,
                "end_ts": end_ts,
            },
            "窗口数": len(rows),
            "已观测窗口数": len(observed),
            "空窗数": len(empty),
            "缺证据数": len(missing),
            "实测零值窗口数": len(zeros),
            "已观测均值": self._mean(observed_values),
            "整期均值": self._mean(observed_values) if complete else None,
            "已观测合计": round(sum(observed_values), 6) if observed else None,
            "整期合计": round(sum(observed_values), 6) if complete else None,
            "完整": complete,
            "可发布": complete,
            "缺证据按零处理": False,
            "去重事件数": len(distinct_events),
            "窗口事件位次数": event_slots,
            "重叠计入次数": max(event_slots - len(distinct_events), 0),
            "重叠窗口": int(window.get("滑动步长") or 0) < int(window.get("滑动宽度") or 0),
            "观测窗口": [self._compact(row) for row in observed],
            "空窗列表": [self._compact(row) for row in empty],
            "缺证据列表": [self._compact(row) for row in missing],
            "说明": (
                "全部窗口均已观测，整期统计可用"
                if complete
                else "存在空窗或缺证据窗口，整期均值/合计不可用；缺证据从未按零计算"
            ),
        }

    @staticmethod
    def _compact(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "窗口指标ID": row.get("id"),
            "窗口起点": row.get("窗口起点"),
            "窗口终点": row.get("窗口终点"),
            "观测状态": row.get("观测状态"),
            "值类型": row.get("值类型"),
            "观测值": row.get("观测值"),
            "事件数": row.get("事件数"),
            "事件编号列表": row.get("事件编号列表", []),
            "缺值原因": row.get("缺值原因"),
            "事件时间基准": row.get("事件时间基准"),
        }

    # -- 发布 --

    def publish_period(
        self,
        *,
        metric_id: str,
        cube_id: Any,
        window_ref: Any = None,
        formula: str = "SUM",
        start: Any = None,
        end: Any = None,
        confirmed_through: Any = None,
    ) -> dict[str, Any]:
        """发布整期指标：窗口结果回写确认清单；有缺口时状态为“发布中断”。"""
        report = self.report_period(
            cube_id=cube_id, window_ref=window_ref, metric_id=metric_id, formula=formula,
            start=start, end=end, confirmed_through=confirmed_through, refresh=True,
        )
        if not report.get("ok"):
            return report
        return self.publish_report(report=report, window_ref=window_ref)

    def publish_report(
        self, *, report: dict[str, Any], window_ref: Any = None
    ) -> dict[str, Any]:
        """基于一份已读出的整期报表发布指标，供跨模块调用避免重复读取整批明细。"""
        if not report.get("ok"):
            return report

        window = _resolve_window(window_ref or report.get("窗口别名"))
        rows = _metric_rows(
            cube_id=report["立方编号"], window=window, metric_id=report["指标编号"],
            start_ts=report["窗口范围"]["start_ts"],
            end_ts=report["窗口范围"]["end_ts"],
        )

        publication = self._build_publication(
            metric_id=report["指标编号"], cube_id=report["立方编号"],
            window=window, rows=rows, report=report,
        )
        store.rows("metric_publication").append(publication)
        self._write_confirmations(publication, rows)
        return {
            "ok": True,
            "report": report,
            "publication": publication,
            "确认清单": self.confirmations_for(publication["发布编号"]),
        }

    def _build_publication(
        self,
        *,
        metric_id: str,
        cube_id: Any,
        window: dict[str, Any] | None,
        rows: list[dict[str, Any]],
        report: dict[str, Any],
    ) -> dict[str, Any]:
        details = []
        linked_events = []
        distinct_events: set[str] = set()
        for row in rows:
            event_nos = list(row.get("事件编号列表", []))
            distinct_events.update(event_nos)
            details.append({
                "窗口度量ID": row.get("id"),
                "窗口起点": row.get("窗口起点"),
                "窗口终点": row.get("窗口终点"),
                "观测状态": row.get("观测状态"),
                "值类型": row.get("值类型"),
                "观测值": row.get("观测值"),
                "事件数": row.get("事件数"),
                "事件编号列表": event_nos,
                "缺值原因": row.get("缺值原因"),
            })
            for event_no in event_nos:
                linked_events.append({
                    "窗口起点": row.get("窗口起点"),
                    "窗口终点": row.get("窗口终点"),
                    "事件编号": event_no,
                })

        submissions = 0
        for event_no in distinct_events:
            event = next(
                (item for item in store.rows("window_event")
                 if str(item.get("立方编号")) == str(cube_id) and str(item.get("事件编号")) == event_no),
                None,
            )
            submissions += int(event.get("重复提交次数", 0)) + 1 if event else 1

        published = sum(1 for row in rows
                        if row.get("观测状态") == "已观测" and row.get("值类型") != "实测零值")
        zeros = sum(1 for row in rows if row.get("值类型") == "实测零值")
        pending = sum(1 for row in rows if row.get("观测状态") != "已观测")
        status = "已发布" if pending == 0 and rows else "发布中断"
        reasons = list(dict.fromkeys(
            str(row.get("缺值原因")) for row in rows
            if row.get("观测状态") != "已观测" and row.get("缺值原因")
        ))

        return {
            "id": max((int(r.get("id", 0)) for r in store.rows("metric_publication")), default=0) + 1,
            "发布编号": f"PUB-{uuid.uuid4().hex[:8].upper()}",
            "指标编号": metric_id,
            "立方编号": str(cube_id),
            "窗口编号": window.get("窗口编号") if window else None,
            "窗口范围": report["窗口范围"],
            "状态": status,
            "尝试次数": 1,
            "分母事件数": len(distinct_events),
            "窗口事件位次数": sum(int(row.get("事件数", 0)) for row in rows),
            "提交记录数": submissions,
            "已发布窗口数": published,
            "实测为零窗口数": zeros,
            "仍缺观测窗口数": pending,
            "中断原因": "；".join(reasons) if reasons else None,
            "整期均值": report["整期均值"],
            "已观测均值": report["已观测均值"],
            "完整": report["完整"],
            "入窗事件": linked_events,
            "窗口明细": details,
            "创建时间": _now_text(),
            "更新时间": _now_text(),
            "status": status,
            "pending": pending > 0,
            "abnormal": status == "发布中断",
        }

    def confirmations_for(self, publication_no: str) -> list[dict[str, Any]]:
        return [
            row for row in store.rows("window_confirmation")
            if str(row.get("发布编号")) == str(publication_no)
        ]

    def _write_confirmations(
        self, publication: dict[str, Any], rows: list[dict[str, Any]], retry: bool = False
    ) -> list[dict[str, Any]]:
        confirmations: list[dict[str, Any]] = []
        for row in rows:
            result = _confirmation_result(row)
            existing = next(
                (item for item in store.rows("window_confirmation")
                 if str(item.get("发布编号")) == publication["发布编号"]
                 and float(item.get("窗口起点秒", -1)) == float(row.get("窗口起点秒", -2))),
                None,
            )
            if existing is None:
                existing = {
                    "id": max((int(r.get("id", 0)) for r in store.rows("window_confirmation")), default=0) + 1,
                    "确认编号": f"CONF-{uuid.uuid4().hex[:8].upper()}",
                    "发布编号": publication["发布编号"],
                    "指标编号": publication["指标编号"],
                    "立方编号": publication["立方编号"],
                    "窗口编号": publication["窗口编号"],
                    "窗口起点秒": row.get("窗口起点秒"),
                    "重试次数": 0,
                    "status": result,
                    "pending": result == "仍缺观测",
                    "abnormal": result == "仍缺观测",
                }
                store.rows("window_confirmation").append(existing)
            if retry:
                existing["重试次数"] = int(existing.get("重试次数", 0)) + 1

            existing.update({
                "窗口起点": row.get("窗口起点"),
                "窗口终点": row.get("窗口终点"),
                "观测状态": row.get("观测状态"),
                "值类型": row.get("值类型"),
                "观测值": row.get("观测值"),
                "事件数": row.get("事件数"),
                "事件编号列表": row.get("事件编号列表", []),
                "缺值原因": row.get("缺值原因"),
                "确认结果": result,
                "确认时间": _now_text(),
                "status": result,
                "pending": result == "仍缺观测",
                "abnormal": result == "仍缺观测",
            })
            confirmations.append(existing)
        return confirmations

    def retry_publication(self, publication_id: Any) -> dict[str, Any]:
        """中断后重试：按窗口快照补算，按事件编号复算分母并回写确认清单。"""
        publication = self._find_publication(publication_id)
        if publication is None:
            return {"ok": False, "message": "发布记录不存在"}

        window = find_window(publication.get("窗口编号"))
        rows = []
        for detail in publication.get("窗口明细", []):
            metric_row_id = detail.get("窗口度量ID")
            if metric_row_id is not None:
                result = self.recompute_window(int(metric_row_id))
                if result.get("ok"):
                    rows.append(result["metric"])
                    continue
            row = self._find_row_by_start(
                publication.get("立方编号"), window, publication.get("指标编号", ""),
                float(detail.get("窗口起点秒", 0) or _seconds(detail.get("窗口起点"))),
            )
            if row is not None:
                self.recompute_window(int(row["id"]))
                rows.append(row)

        rows.sort(key=lambda item: float(item.get("窗口起点秒", 0)))
        # 用发布时锁定的窗口范围读取报表，重试不会因为新增了更晚的窗口而改变分母。
        report = self.report_period(
            cube_id=publication["立方编号"], window_ref=window,
            metric_id=publication.get("指标编号", ""),
            formula=str(rows[0].get("公式", "SUM")) if rows else "SUM",
            start=publication["窗口范围"].get("start"),
            end=publication["窗口范围"].get("end"),
            refresh=False,
        )

        # 按事件编号去重后复算分母，重复提交不会放大分母。
        distinct_events = {event_no for row in rows for event_no in row.get("事件编号列表", [])}
        submissions = 0
        for event_no in distinct_events:
            event = next(
                (item for item in store.rows("window_event")
                 if str(item.get("立方编号")) == str(publication["立方编号"])
                 and str(item.get("事件编号")) == event_no),
                None,
            )
            submissions += int(event.get("重复提交次数", 0)) + 1 if event else 1

        published = sum(1 for row in rows
                        if row.get("观测状态") == "已观测" and row.get("值类型") != "实测零值")
        zeros = sum(1 for row in rows if row.get("值类型") == "实测零值")
        pending = sum(1 for row in rows if row.get("观测状态") != "已观测")
        status = "已重试完成" if pending == 0 and rows else "发布中断"

        publication.update({
            "状态": status,
            "尝试次数": int(publication.get("尝试次数", 1)) + 1,
            "分母事件数": len(distinct_events),
            "窗口事件位次数": sum(int(row.get("事件数", 0)) for row in rows),
            "提交记录数": submissions,
            "已发布窗口数": published,
            "实测为零窗口数": zeros,
            "仍缺观测窗口数": pending,
            "整期均值": report.get("整期均值"),
            "已观测均值": report.get("已观测均值"),
            "完整": report.get("完整", False),
            "窗口明细": [
                {
                    "窗口度量ID": row.get("id"),
                    "窗口起点": row.get("窗口起点"),
                    "窗口终点": row.get("窗口终点"),
                    "观测状态": row.get("观测状态"),
                    "值类型": row.get("值类型"),
                    "观测值": row.get("观测值"),
                    "事件数": row.get("事件数"),
                    "事件编号列表": row.get("事件编号列表", []),
                    "缺值原因": row.get("缺值原因"),
                }
                for row in rows
            ],
            "入窗事件": [
                {"窗口起点": row.get("窗口起点"), "窗口终点": row.get("窗口终点"), "事件编号": event_no}
                for row in rows for event_no in row.get("事件编号列表", [])
            ],
            "中断原因": "；".join(sorted({
                str(row.get("缺值原因")) for row in rows
                if row.get("观测状态") != "已观测" and row.get("缺值原因")
            })) or None,
            "更新时间": _now_text(),
            "status": status,
            "pending": pending > 0,
            "abnormal": status == "发布中断",
        })

        confirmations = self._write_confirmations(publication, rows, retry=True)
        return {
            "ok": True,
            "重试": True,
            "report": report,
            "publication": publication,
            "确认结果统计": {
                "已发布": published,
                "实测为零": zeros,
                "仍缺观测": pending,
            },
            "确认清单": confirmations,
            "分母事件数": len(distinct_events),
            "提交记录数": submissions,
        }

    def _find_publication(self, ref: Any) -> dict[str, Any] | None:
        target = str(ref)
        for row in store.rows("metric_publication"):
            if str(row.get("id")) == target or str(row.get("发布编号")) == target:
                return row
        return None

    def _find_row_by_start(
        self, cube_id: Any, window: dict[str, Any] | None, metric_id: str, start_ts: float
    ) -> dict[str, Any] | None:
        for row in store.rows("sparse_window_metric"):
            if (
                str(row.get("立方编号")) == str(cube_id)
                and str(row.get("指标编号", "")) == metric_id
                and (window is None or str(row.get("窗口定义", "")) == str(window.get("窗口编号", "")))
                and float(row.get("窗口起点秒", -1)) == start_ts
            ):
                return row
        return None

    # -- 查询 --

    def publication_events(self, publication_id: Any) -> dict[str, Any]:
        """把发布记录关联回入窗事件，便于按事件编号核对。"""
        publication = self._find_publication(publication_id)
        if publication is None:
            return {"ok": False, "message": "发布记录不存在"}

        event_index = {
            (str(row.get("立方编号")), str(row.get("事件编号"))): row
            for row in store.rows("window_event")
        }
        linked = []
        for item in publication.get("入窗事件", []):
            event = event_index.get((str(publication["立方编号"]), str(item.get("事件编号"))))
            linked.append({
                "窗口起点": item.get("窗口起点"),
                "窗口终点": item.get("窗口终点"),
                "事件编号": item.get("事件编号"),
                "事件时间": event.get("事件时间") if event else None,
                "观测值": event.get("观测值") if event else None,
                "重复提交次数": int(event.get("重复提交次数", 0)) if event else 0,
                "迟到": bool(event.get("迟到")) if event else False,
            })
        return {
            "ok": True,
            "publication_id": publication["id"],
            "发布编号": publication["发布编号"],
            "分母事件数": publication.get("分母事件数"),
            "提交记录数": publication.get("提交记录数"),
            "events": linked,
        }

    def list_metrics(
        self,
        *,
        cube_id: Any | None = None,
        window_ref: Any | None = None,
        metric_id: str | None = None,
        state: str | None = None,
    ) -> list[dict[str, Any]]:
        window = find_window(window_ref) if window_ref not in (None, "") else None
        rows = list(store.rows("sparse_window_metric"))
        if cube_id is not None:
            rows = [row for row in rows if str(row.get("立方编号")) == str(cube_id)]
        if window is not None:
            rows = [row for row in rows if str(row.get("窗口定义")) == str(window.get("窗口编号"))]
        if metric_id is not None:
            rows = [row for row in rows if str(row.get("指标编号", "")) == str(metric_id)]
        if state:
            rows = [row for row in rows if row.get("观测状态") == state]
        return sorted(rows, key=lambda row: float(row.get("窗口起点秒", 0)))

    def list_publications(self) -> list[dict[str, Any]]:
        return list(store.rows("metric_publication"))

    def list_confirmations(
        self, *, publication_no: str | None = None, result: str | None = None
    ) -> list[dict[str, Any]]:
        rows = list(store.rows("window_confirmation"))
        if publication_no is not None:
            rows = [row for row in rows if str(row.get("发布编号")) == str(publication_no)]
        if result:
            rows = [row for row in rows if row.get("确认结果") == result]
        return rows


def _confirmation_result(row: dict[str, Any]) -> str:
    if row.get("观测状态") != "已观测":
        return "仍缺观测"
    if row.get("值类型") == "实测零值":
        return "实测为零"
    return "已发布"


# 便捷单例：路由与跨模块调用共用同一份内存状态。
_service = SparseWindowMetricService()


def ingest_event(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _service.ingest_event(*args, **kwargs)


def compute_windows(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _service.compute_windows(*args, **kwargs)


def report_period(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _service.report_period(*args, **kwargs)


def publish_period(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _service.publish_period(*args, **kwargs)


def publish_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _service.publish_report(*args, **kwargs)


def retry_publication(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _service.retry_publication(*args, **kwargs)
