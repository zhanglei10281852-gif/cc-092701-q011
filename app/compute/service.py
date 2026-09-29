from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, PermissionDeniedError, ValidationError
from app.database import get_connection, transaction
from app.repositories.audit import AuditRepository


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return dict(repository.task_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_task(
                template_id=template["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidate = repository.queued_candidate(capabilities, now)
            if candidate is None:
                return None
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            return dict(repository.task_by_id(candidate["id"]))

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            return dict(ComputeRepository(connection).task_by_id(task_id))

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
            )
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            connection.execute(
                "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code, message[:2000], None if can_retry else now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        action, mutation = self._intervention_mutation("retry", priority)
        return self._intervene(task_id, actor, reason, action, batch_key, mutation)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        action, mutation = self._intervention_mutation("priority", priority)
        return self._intervene(task_id, actor, reason, action, batch_key, mutation)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    # ------------------------------------------------------------------
    # 两阶段“先查看、后确认”批量协议
    # ------------------------------------------------------------------

    PROTOCOL_ALLOWED_STATUS = {
        "cancel": {"queued", "running"},
        "retry": {"failed", "cancelled"},
        "priority": {"queued", "running"},
    }
    PROTOCOL_REJECT_MESSAGES = {
        "cancel": "当前任务状态不允许取消",
        "retry": "只有失败或已取消任务可以人工重试",
        "priority": "只有排队或运行中的任务可以调整优先级",
    }

    def preview_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires_at = to_storage(now_value + timedelta(seconds=int(payload["ttl_seconds"])))
        operation = payload["operation"]
        selection, ordered_ids = self._resolve_selection(payload)
        if not ordered_ids:
            raise ValidationError("固定筛选条件后没有命中任何任务")
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            items = [self._evaluate_item(repository, task_id, operation, position) for position, task_id in enumerate(ordered_ids)]
            allowed_count = sum(1 for item in items if item["allowed"])
            summary = {
                "actor": payload["actor"],
                "operation": operation,
                "reason": payload["reason"],
                "priority": payload.get("priority"),
                "execution_mode": payload["execution_mode"],
                "selection": selection,
                "items": [
                    {
                        "task_id": item["task_id"],
                        "version": item["version"],
                        "allowed": item["allowed"],
                        "action": item["action"],
                        "reject_reason": item["reject_reason"],
                    }
                    for item in items
                ],
            }
            preview_digest = digest(summary)
            token = secrets.token_urlsafe(32)
            run = repository.create_protocol_run(
                token_digest=self._token_digest(token),
                actor=payload["actor"],
                operation=operation,
                reason=payload["reason"],
                priority=payload.get("priority"),
                execution_mode=payload["execution_mode"],
                selection=selection,
                preview_digest=preview_digest,
                task_count=len(items),
                allowed_count=allowed_count,
                rejected_count=len(items) - allowed_count,
                previewed_at=now,
                expires_at=expires_at,
            )
            run_id = int(run["id"])
            for item in items:
                repository.add_protocol_item(
                    run_id=run_id,
                    task_id=item["task_id"],
                    position=item["position"],
                    task_version=item["version"],
                    task_status=item["status"],
                    allowed=item["allowed"],
                    allowed_action=item["action"],
                    reject_reason=item["reject_reason"],
                )
            AuditRepository(connection).append(
                actor_user_id=None,
                actor_name=payload["actor"],
                action="compute.batch_protocol.preview",
                resource_type="compute_batch_protocol_run",
                resource_id=run_id,
                outcome="success",
                before=None,
                after={"task_count": len(items), "allowed_count": allowed_count, "rejected_count": len(items) - allowed_count},
                metadata={
                    "operation": operation,
                    "execution_mode": payload["execution_mode"],
                    "selection": selection,
                    "preview_digest": preview_digest,
                    "expires_at": expires_at,
                },
                correlation_id=f"batch-protocol-{run_id}",
                created_at=now,
            )
        return {
            "run_id": run_id,
            "token": token,
            "preview_digest": preview_digest,
            "operation": operation,
            "actor": payload["actor"],
            "reason": payload["reason"],
            "priority": payload.get("priority"),
            "execution_mode": payload["execution_mode"],
            "selection": selection,
            "previewed_at": now,
            "expires_at": expires_at,
            "task_count": len(items),
            "allowed_count": allowed_count,
            "rejected_count": len(items) - allowed_count,
            "items": [self._public_item(item) for item in items],
        }

    def confirm_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            with transaction(immediate=True) as connection:
                return self._confirm_batch_in_tx(ComputeRepository(connection), connection, payload)
        except (ConflictError, PermissionDeniedError) as exc:
            self._record_confirmation_denial(payload, exc)
            raise

    def get_protocol_run(self, token: str) -> dict[str, Any]:
        row = self.repository.protocol_run_by_token_digest(self._token_digest(token))
        if row is None:
            raise NotFoundError("批量协议凭据不存在", code="batch_protocol_token_invalid")
        run = dict(row)
        now = to_storage(self.clock.now())
        effective_status = run["status"]
        if effective_status == "open" and now > run["expires_at"]:
            effective_status = "expired"
        items = [dict(item) for item in self.repository.protocol_items(int(run["id"]))]
        return {
            "run_id": int(run["id"]),
            "status": effective_status,
            "operation": run["operation"],
            "actor": run["actor"],
            "reason": run["reason"],
            "priority": run["priority"],
            "execution_mode": run["execution_mode"],
            "selection": json.loads(run["selection_json"]),
            "preview_digest": run["preview_digest"],
            "previewed_at": run["previewed_at"],
            "expires_at": run["expires_at"],
            "confirmed_at": run["confirmed_at"],
            "confirmed_by": run["confirmed_by"],
            "committed_count": int(run["committed_count"]),
            "skipped_count": int(run["skipped_count"]),
            "result": json.loads(run["result_json"] or "{}"),
            "drift": json.loads(run["drift_json"] or "{}"),
            "items": [
                {
                    "task_id": int(item["task_id"]),
                    "position": int(item["position"]),
                    "version": item["task_version"],
                    "status": item["task_status"],
                    "allowed": bool(item["allowed"]),
                    "action": item["allowed_action"],
                    "reject_reason": item["reject_reason"],
                    "outcome": item["outcome"] or None,
                    "actual_status": item["actual_status"] or None,
                    "actual_version": item["actual_version"],
                    "intervention_id": item["intervention_id"],
                    "detail": item["detail"] or None,
                }
                for item in items
            ],
        }

    def _confirm_batch_in_tx(self, repository: ComputeRepository, connection: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        token = payload["token"]
        run_row = repository.protocol_run_by_token_digest(self._token_digest(token))
        if run_row is None:
            raise NotFoundError("批量协议凭据不存在", code="batch_protocol_token_invalid")
        run = dict(run_row)
        run_id = int(run["id"])
        if run["actor"] != payload["actor"]:
            raise PermissionDeniedError("确认人与预览发起人不一致", context={"run_id": run_id, "preview_actor": run["actor"]}, code="batch_protocol_actor_mismatch")
        if run["status"] != "open":
            raise ConflictError("确认凭据只能消费一次", context={"run_id": run_id, "status": run["status"]}, code="batch_protocol_token_consumed")
        if run["preview_digest"] != payload["preview_digest"]:
            raise ConflictError(
                "预览摘要不匹配，确认的不是最近一次预览",
                context={"run_id": run_id, "expected_preview_digest": run["preview_digest"]},
                code="batch_protocol_digest_mismatch",
            )

        operation = run["operation"]
        priority = run["priority"]
        reason = run["reason"]
        actor = payload["actor"]
        mode = run["execution_mode"]
        preview_items = [dict(item) for item in repository.protocol_items(run_id)]
        current_snapshots = {item["task_id"]: self._evaluate_item(repository, int(item["task_id"]), operation, int(item["position"])) for item in preview_items}

        drift: list[dict[str, Any]] = []
        for preview in preview_items:
            current = current_snapshots[int(preview["task_id"])]
            changes: dict[str, Any] = {}
            if current["version"] is None:
                changes["deleted"] = {"preview": False, "current": True}
            else:
                if preview["task_version"] is None:
                    changes["deleted"] = {"preview": True, "current": False}
                elif current["version"] != int(preview["task_version"]):
                    changes["version"] = {"preview": int(preview["task_version"]), "current": current["version"]}
                if current["status"] != preview["task_status"]:
                    changes["status"] = {"preview": preview["task_status"], "current": current["status"]}
            if changes:
                changes.update({"currently_allowed": current["allowed"], "reject_reason": current["reject_reason"]})
                drift.append({"task_id": int(preview["task_id"]), "changes": changes})

        # 过期同样返回逐项差异，便于调用方据此重新发起预览。
        if now > run["expires_at"]:
            raise ConflictError(
                "预览已过期，请重新预览后再确认",
                context={"run_id": run_id, "previewed_at": run["previewed_at"], "expires_at": run["expires_at"], "confirmed_at": now, "drift": drift},
                code="batch_protocol_expired",
            )

        if mode == "abort" and drift:
            raise ConflictError(
                "确认时发现任务状态相对预览已漂移，本次未做任何修改",
                context={"run_id": run_id, "drift": drift},
                code="batch_protocol_drift",
            )

        # 原子消费凭据：只有仍开放的凭据可以走到这里，杜绝重复确认。
        if repository.consume_protocol_token(self._token_digest(token), now=now) != 1:
            raise ConflictError("确认凭据只能消费一次", context={"run_id": run_id}, code="batch_protocol_token_consumed")

        batch_key = f"protocol:{run_id}"
        committed: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        action, mutation = self._intervention_mutation(operation, priority)
        for preview in preview_items:
            task_id = int(preview["task_id"])
            current = current_snapshots[task_id]
            if not bool(preview["allowed"]):
                skip_reason = f"预览时已拒绝：{preview['reject_reason']}" if preview["reject_reason"] else "预览时已拒绝"
                repository.record_protocol_item_outcome(
                    run_id, task_id, outcome="skipped", actual_status=current["status"],
                    actual_version=current["version"], intervention_id=None, detail=skip_reason,
                )
                skipped.append({"task_id": task_id, "reason": skip_reason, "version": current["version"], "status": current["status"]})
                continue
            if current["version"] is None:
                skip_reason = "任务在确认时已不存在"
            elif current["version"] != int(preview["task_version"]):
                skip_reason = f"版本已漂移：v{preview['task_version']}→v{current['version']}"
            elif not current["allowed"]:
                skip_reason = f"状态已变化：{current['reject_reason']}"
            else:
                skip_reason = ""
            if skip_reason:
                repository.record_protocol_item_outcome(
                    run_id, task_id, outcome="skipped", actual_status=current["status"],
                    actual_version=current["version"], intervention_id=None, detail=skip_reason,
                )
                skipped.append({"task_id": task_id, "reason": skip_reason, "version": current["version"], "status": current["status"]})
                continue
            task = repository.task_by_id(task_id)
            after, intervention_id = self._run_intervention_in_tx(
                repository, connection, task, actor=actor, reason=reason, action=action,
                mutation=mutation, batch_key=batch_key, now=now,
            )
            repository.record_protocol_item_outcome(
                run_id, task_id, outcome="committed", actual_status=after["status"],
                actual_version=int(after["version"]), intervention_id=intervention_id, detail="",
            )
            committed.append({"task_id": task_id, "status": after["status"], "version": int(after["version"]), "intervention_id": intervention_id})

        result = {"committed": committed, "skipped": skipped}
        drift_payload = {"items": drift}
        repository.finalize_protocol_run(
            run_id, committed_count=len(committed), skipped_count=len(skipped),
            result=result, drift=drift_payload,
        )
        # 记录确认人；与预览审计共用 correlation_id，且逐项干预通过 batch_key/intervention_id 串联。
        connection.execute(
            "UPDATE compute_batch_protocol_runs SET confirmed_by=? WHERE id=?",
            (actor, run_id),
        )
        AuditRepository(connection).append(
            actor_user_id=None,
            actor_name=actor,
            action="compute.batch_protocol.confirm",
            resource_type="compute_batch_protocol_run",
            resource_id=run_id,
            outcome="success",
            before={"allowed_count": int(run["allowed_count"]), "rejected_count": int(run["rejected_count"])},
            after={"committed_count": len(committed), "skipped_count": len(skipped)},
            metadata={
                "operation": operation,
                "execution_mode": mode,
                "preview_digest": run["preview_digest"],
                "committed": committed,
                "skipped": [{"task_id": item["task_id"], "reason": item["reason"]} for item in skipped],
                "drift": drift_payload,
            },
            correlation_id=f"batch-protocol-{run_id}",
            created_at=now,
        )
        return {
            "run_id": run_id,
            "status": "committed",
            "operation": operation,
            "execution_mode": mode,
            "preview_digest": run["preview_digest"],
            "committed_count": len(committed),
            "skipped_count": len(skipped),
            "committed": committed,
            "skipped": skipped,
            "drift": drift_payload,
            "confirmed_by": actor,
            "confirmed_at": now,
        }

    def _resolve_selection(self, payload: dict[str, Any]) -> tuple[dict[str, Any], list[int]]:
        """把显式列表或筛选条件固定为有序、去重的任务集合。"""
        if payload.get("task_ids") is not None:
            ordered_ids = list(dict.fromkeys(int(task_id) for task_id in payload["task_ids"]))
            selection = {"mode": "task_ids", "task_ids": ordered_ids}
            return selection, ordered_ids
        filters = payload["filter"]
        rows = self.repository.list_tasks(
            status=filters.get("status"),
            project_code=filters.get("project_code"),
            requested_by=filters.get("requested_by"),
            limit=int(filters["limit"]),
        )
        ordered_ids = [int(row["id"]) for row in rows]
        selection = {
            "mode": "filter",
            "filter": {
                "status": filters.get("status"),
                "project_code": filters.get("project_code"),
                "requested_by": filters.get("requested_by"),
                "limit": int(filters["limit"]),
            },
            "task_ids": ordered_ids,
        }
        return selection, ordered_ids

    def _evaluate_item(self, repository: ComputeRepository, task_id: int, operation: str, position: int) -> dict[str, Any]:
        task = repository.task_by_id(task_id)
        if task is None:
            return {
                "task_id": task_id, "position": position, "version": None, "status": "",
                "allowed": False, "action": "", "reject_reason": "计算任务不存在",
            }
        status = str(task["status"])
        allowed = status in self.PROTOCOL_ALLOWED_STATUS[operation]
        return {
            "task_id": task_id,
            "position": position,
            "version": int(task["version"]),
            "status": status,
            "allowed": allowed,
            "action": operation if allowed else "",
            "reject_reason": "" if allowed else self.PROTOCOL_REJECT_MESSAGES[operation],
        }

    @staticmethod
    def _public_item(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "task_id": item["task_id"],
            "position": item["position"],
            "version": item["version"],
            "status": item["status"],
            "allowed": item["allowed"],
            "action": item["action"],
            "reject_reason": item["reject_reason"],
        }

    @staticmethod
    def _token_digest(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def _record_confirmation_denial(self, payload: dict[str, Any], exc: ConflictError) -> None:
        """漂移/过期等拒绝在主事务回滚后，单独落一条 denied 审计，便于事后解释。"""
        context = exc.context or {}
        run_id = context.get("run_id")
        try:
            now = to_storage(self.clock.now())
            with transaction(immediate=True) as connection:
                AuditRepository(connection).append(
                    actor_user_id=None,
                    actor_name=payload.get("actor") or "unknown",
                    action="compute.batch_protocol.confirm",
                    resource_type="compute_batch_protocol_run",
                    resource_id=run_id,
                    outcome="denied",
                    before=None,
                    after=None,
                    metadata={"reason_code": exc.code, **{key: value for key, value in context.items() if key != "run_id"}},
                    correlation_id=f"batch-protocol-{run_id}" if run_id is not None else None,
                    created_at=now,
                )
        except Exception:  # 审计失败不能掩盖原始业务拒绝
            pass

    def _intervention_mutation(self, operation: str, priority: int | None) -> tuple[str, Callable[[sqlite3.Connection, sqlite3.Row, str], None]]:
        if operation == "cancel":
            return "cancel", self._cancel_mutation

        if operation == "retry":
            def retry_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
                if task["status"] not in {"failed", "cancelled"}:
                    raise ConflictError("只有失败或已取消任务可以人工重试")
                chosen = task["priority"] if priority is None else priority
                connection.execute(
                    "UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?",
                    (chosen, now, now, task["id"]),
                )
            return "retry", retry_mutation

        def priority_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute(
                "UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?",
                (int(priority), now, task["id"]),
            )
        return "priority", priority_mutation

    @staticmethod
    def _run_intervention_in_tx(
        repository: ComputeRepository,
        connection: sqlite3.Connection,
        task: sqlite3.Row,
        *,
        actor: str,
        reason: str,
        action: str,
        mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None],
        batch_key: str,
        now: str,
    ) -> tuple[dict[str, Any], int]:
        before = dict(task)
        mutation(connection, task, now)
        after = dict(repository.task_by_id(int(task["id"])))
        intervention_id = repository.add_intervention(
            task_id=int(task["id"]), actor=actor, action=action, reason=reason,
            before=before, after=after, batch_key=batch_key, now=now,
        )
        return after, intervention_id


    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute("SELECT * FROM compute_tasks WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id", (now,)).fetchall()
            for task in rows:
                before = dict(task)
                if int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, finished_at, now, task["id"]),
                )
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, task, now)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
        if task["status"] not in {"queued", "running"}:
            raise ConflictError("当前任务状态不允许取消")
        status = "cancel_requested" if task["status"] == "running" else "cancelled"
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))

    def _check_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队任务配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行任务配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized
