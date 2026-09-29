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

## 先查看、后确认的批量操作

直接调用旧接口 `POST /api/compute/tasks/batch` 会逐条开事务，任务状态中途变化时留下半批成功。需要整批处理时使用两阶段协议：

1. 预览 `POST /api/compute/tasks/batch-preview`：固定 `task_ids` 或筛选条件（`status`/`project_code`/`requested_by`，二者互斥），逐项返回当前 `status`、`version`、`allowed_action` 与 `reject_reason`，并给出一次性凭据 `preview_key`、摘要 `digest`、`summary` 和 `expires_at`（默认 10 分钟）。
2. 确认 `POST /api/compute/tasks/batch-confirm`：回传 `preview_key`、预览得到的 `expected_digest` 与执行模式：
   - `atomic`：全部项目预览时即允许且确认时版本无漂移才在单事务内提交，否则整体中止、不留半截结果；
   - `partial`：明确接受逐条结果，允许的项目执行，拒绝项以 `batch_skip` 干预记录逐条留痕。

凭据只能消费一次：重复确认返回首次消费人与结果；过期会把凭据置为 `expired`；预览后任务版本或状态漂移时返回逐项 `drift` 差异（预览值 vs 当前值），凭据不被消费，可重新预览后再确认。`audit_events` 以 `correlation_id=preview_key` 串联预览事件、确认人、确认结果与拒绝原因，每项实际变化或跳过写入 `compute_interventions`（`batch_key=preview_key`），事后可完整解释某条为何被执行或跳过。
