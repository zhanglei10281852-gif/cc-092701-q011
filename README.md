# 职业教学任务运营服务

这是一个面向职业院校教务团队、授课教师和课程管理员的 Python 后端服务，用于管理课程任务模板、学员提交、执行队列、教师工作者、结果版本和运营干预。服务保留登录、角色权限、会话、审计和配额等基础能力，所有业务状态与审计事件写入本地 SQLite 数据库，适合在单个应用容器中离线运行。

## 运行环境

- Python 3.11
- SQLite 3（由 Python 标准库提供）
- FastAPI 与 Uvicorn

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/compute-operations.db`，可复制 `.env.example` 并设置 `TOWNSHIP_DATABASE_PATH` 指向其他本地路径。

## 初始化与启动

```bash
python -m app.cli init-db
python -m app.cli check-db
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

课程任务运营接口使用 `/api/compute` 前缀，身份、角色、审计和系统接口分别位于 `/api/auth`、`/api/roles`、`/api/audit` 与 `/api/system`。

## 两阶段批量操作协议（先查看、后确认）

直接调用旧的 `POST /api/compute/tasks/batch` 会逐条独立提交，任务状态在中途变化时会留下“半批成功”。需要稳妥处理一批任务时，改用预览—确认协议：

1. `POST /api/compute/tasks/batch-protocol/preview`
   - 通过 `task_ids` 显式列表或 `filter`（status/project_code/requested_by）选择任务；命中的任务集合与筛选条件在预览时被**冻结**。
   - 逐项返回当前 `version`、`allowed`（是否允许该动作）、`action` 与 `reject_reason`（拒绝理由）。
   - 返回一次性 `token`（服务端只存摘要）、`preview_digest`（预览摘要）与 `expires_at`。
   - `execution_mode`：`abort`（默认，确认时若有任何漂移则整体回滚、全部成功才提交）或 `accept_partial`（明确接受逐条结果，漂移项被跳过）。
2. `POST /api/compute/tasks/batch-protocol/confirm`
   - 必须回传 `token`、`preview_digest` 与预览发起人 `actor`；服务端核对摘要、确认人和逐项版本。
   - `token` 在单个即时事务中**只能消费一次**；摘要不符、确认人不符、过期或状态漂移都会被拒绝。
   - 过期或漂移时在错误 `context.drift` 中逐项给出 `version`/`status` 的预览值与当前值；`abort` 模式下不做任何修改。
3. `GET /api/compute/tasks/batch-protocol/runs/{token}`
   - 事后追溯：返回预览摘要、确认人、每项的实际状态变化、对应的 `intervention_id` 以及被跳过的原因。

审计上，预览与确认分别写入 `audit_events` 并以同一 `correlation_id` 关联；每项实际变化写入 `compute_interventions`，其 `batch_key=protocol:<run_id>` 与逐项记录的 `intervention_id` 串联，从而能解释某条为什么被提交或跳过；被拒绝的确认也会留下 `outcome=denied` 的审计记录。

## 测试与编译检查

```bash
python -m pytest
python -m compileall -q app tests
```

本地冒烟命令：

```bash
python -m app.cli smoke
python -m app.cli compute-demo
```

## 目录结构

```text
app/compute/       任务模板、配额、提交、领取、回执和人工干预
app/api/            登录、角色、审计和系统管理接口
app/core/           时钟、安全、异常和分页能力
app/repositories/   SQLite 查询与事务封装
app/services/       身份、审计和后台任务服务
app/database.py     SQLite 连接、事务、表结构和权限初始化
tests/              领域、接口、调度和身份回归测试
tools/              本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL 和忙等待策略。提交、领取、回执和人工干预在即时事务中完成；租约、配额与结果版本使用可注入时钟，便于复现跨日和恢复边界。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。
