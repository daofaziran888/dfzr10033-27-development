# 湖仓工坊 · 联邦查询与多维自助分析平台

面向异构数据源联邦查询、语义模型构建、OLAP多维立方物化、聚合缓存预热、时序窗口分析、查询改写优化与数据集市发布的多维自助分析平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/sparse_window_metric.py  稀疏分窗指标域（内部数据表）
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据源注册 | `source_registry` | 注册数据源 | 源编号、源类型、连接地址 |
| 语义模型 | `semantic_model` | 语义定义 | 模型编号、模型名称、源表映射 |
| 联邦查询 | `query_federation` | 联邦计划 | 计划编号、参与来源、关联键 |
| 多维立方 | `olap_cube` | OLAP立方 | 立方编号、立方名称、维度列表 |
| 指标工厂 | `metric_factory` | 计算指标 | 指标编号、指标名称、数据来源 |
| 维度树 | `dimension_tree` | 维度层级 | 层级编号、维度名称、层级路径 |
| 聚合缓存 | `aggregate_cache` | 缓存配置 | 配置编号、目标立方、缓存粒度 |
| 查询改写 | `query_rewrite` | 改写规则 | 规则编号、原始模式、替换模式 |
| 时序窗口 | `time_window` | 窗口定义 | 窗口编号、窗口类型、滑动宽度 |
| 数据切片 | `dataset_slice` | 切片配置 | 切片编号、源立方、维度过筛 |
| 下钻配置 | `drill_config` | 下钻路径 | 路径编号、起始维度、目标维度 |
| 结果集市 | `result_mart` | 数据Mart | 集市编号、集市名称、数据来源 |
| 基准集 | `benchmark_set` | 分析基线 | 基线编号、基线名称、数据范围 |
| 查询审计 | `query_audit` | 审计记录 | 记录编号、查询语句、执行耗时 |
| 成本治理 | `cost_govern` | 配额策略 | 策略编号、适用角色、查询上限 |
| 新鲜度监控 | `freshness_mon` | 新鲜度规则 | 规则编号、目标数据集、SLA秒数 |
| 分析导出 | `data_exporter` | 导出任务 | 任务编号、数据来源、筛选条件 |
| 定时计算 | `schedule_job` | 预计算任务 | 任务编号、目标立方、Cron表达式 |
| 湖仓读取 | `data_lake_read` | 湖仓路径 | 路径编号、存储位置、文件格式 |
| 结果缓存 | `result_cache` | 缓存条目 | 条目编号、查询摘要、结果哈希 |
| 数据契约 | `data_contract` | 契约定义 | 契约编号、数据集、Schema约束 |
| 实验配置 | `ab_config` | AB实验 | 实验编号、实验名称、分流比例 |

## 稀疏分窗指标

指标工厂不再直接读取整批聚合明细自行分窗，而是消费 `sparse_window_metric`
里按事件时间切好的稀疏窗口结果，再把发布结论回写 `window_confirmation`。

| 接口 | 说明 |
| --- | --- |
| `POST /api/window-metrics/events` | 登记带事件时间的立方明细（事件编号在立方内唯一） |
| `POST /api/window-metrics/compute` | 按窗口定义切分窗口，写入窗口指标 |
| `GET /api/window-metrics` | 读取窗口指标（可按立方/窗口/状态筛选） |
| `GET /api/window-metrics/report` | 业务报表：已观测时段统计 + 整期是否完整 |
| `POST /api/window-metrics/{id}/recompute` | 按窗口快照补算单个窗口 |
| `POST /api/window-metrics/publish` | 发布整期指标并回写确认清单 |
| `GET /api/window-metrics/publications` | 发布记录 |
| `GET /api/window-metrics/publications/{id}` | 发布记录关联回入窗事件 |
| `POST /api/window-metrics/publications/{id}/retry` | 中断后按事件编号复算分母重试 |
| `GET /api/window-metrics/confirmations` | 窗口确认清单（已发布/实测为零/仍缺观测） |
| `POST /api/pipelines/metric-window-publish` | 指标工厂跨模块发布管线 |

窗口状态语义：

- `已观测`：窗口内有事件，`值类型` 为 `实测值` 或 `实测零值`。
- `空窗`：夹在两次已观测事件之间、本窗口没有事件，`观测值` 为 `null`，
  保留 `缺值原因`，不会记为 0。
- `缺证据`：超出已确认事件时间范围或完全没有事件，同样 `观测值` 为 `null`。

只要窗口集合里存在 `空窗` 或 `缺证据`，`整期均值` 就是 `null`；迟到的真实
零值入窗后转为 `实测零值`，整期均值才会把它计入。补算只使用窗口行快照里的
`对齐基准`、`滑动宽度`、`事件时间基准`，事后修改窗口配置不会移动既有边界。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据分析与数据应用
