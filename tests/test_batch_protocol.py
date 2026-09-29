from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError, PermissionDeniedError
from app.database import get_connection


TEMPLATE = {
    "code": "solver-b",
    "name": "批量协议求解模板",
    "algorithm": "solver-b",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def create_template(client) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text


def submit_task(client, key: str, *, user: str = "teacher-1", priority: int = 50) -> dict:
    response = client.post(
        "/api/compute/tasks",
        json={
            "template_code": "solver-b",
            "project_code": "lab-a",
            "requested_by": user,
            "parameters": {"iterations": 10, "mode": "fast"},
            "priority": priority,
            "idempotency_key": key,
        },
    )
    assert response.status_code == 202, response.text
    return response.json()


def make_succeeded(client, task_id: int) -> None:
    # 目标任务此前以高优先级提交，确保领取时排在其它排队任务之前
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-b"], "lease_seconds": 60})
    assert claimed.status_code == 200 and claimed.json()["task"]["id"] == task_id
    completed = client.post(
        f"/api/compute/tasks/{task_id}/complete",
        json={"worker_id": "w1", "result": {"value": 1}, "metrics": {}},
    )
    assert completed.status_code == 200


def preview(client, **overrides) -> dict:
    payload = {
        "operation": "cancel",
        "actor": "admin-a",
        "reason": "实训被误标为待重做，统一撤回",
        "task_ids": [],
        "execution_mode": "abort",
    }
    payload.update(overrides)
    response = client.post("/api/compute/tasks/batch-protocol/preview", json=payload)
    assert response.status_code == 200, response.text
    return response.json()


def confirm(client, token: str, digest: str, actor: str = "admin-a"):
    return client.post(
        "/api/compute/tasks/batch-protocol/confirm",
        json={"token": token, "preview_digest": digest, "actor": actor},
    )


def test_preview_freezes_selection_and_reports_version_action_rejection(client):
    create_template(client)
    t1 = submit_task(client, "bp-0001")
    t2 = submit_task(client, "bp-0002")
    # 高优先级确保该任务最先被领取，从而变为 succeeded 用于制造拒绝项
    t3 = submit_task(client, "bp-0003", priority=90)
    make_succeeded(client, t3["id"])

    result = preview(client, task_ids=[t1["id"], t2["id"], t3["id"]])
    assert result["task_count"] == 3
    assert result["allowed_count"] == 2 and result["rejected_count"] == 1
    by_id = {item["task_id"]: item for item in result["items"]}
    assert by_id[t1["id"]]["version"] == 1
    assert by_id[t1["id"]]["allowed"] is True
    assert by_id[t1["id"]]["action"] == "cancel"
    assert by_id[t1["id"]]["reject_reason"] == ""
    assert by_id[t3["id"]]["allowed"] is False
    assert by_id[t3["id"]]["action"] == ""
    assert "不允许取消" in by_id[t3["id"]]["reject_reason"]
    assert len(result["token"]) >= 32 and len(result["preview_digest"]) == 64
    assert result["expires_at"] > result["previewed_at"]
    # 筛选条件被原样固定并回显
    assert result["selection"]["mode"] == "task_ids"
    assert result["selection"]["task_ids"] == [t1["id"], t2["id"], t3["id"]]


def test_filter_selection_is_frozen_at_preview_time(client):
    create_template(client)
    t1 = submit_task(client, "bp-filter-1")
    result = preview(
        client,
        task_ids=None,
        filter={"status": "queued", "project_code": "lab-a", "limit": 50},
    )
    assert result["selection"]["mode"] == "filter"
    assert result["selection"]["task_ids"] == [t1["id"]]
    # 预览之后新入队的任务不属于本次冻结集合
    later = submit_task(client, "bp-filter-2")
    response = confirm(client, result["token"], result["preview_digest"])
    assert response.status_code == 200, response.text
    committed_ids = [item["task_id"] for item in response.json()["committed"]]
    assert committed_ids == [t1["id"]]
    assert later["id"] not in committed_ids


def test_abort_mode_commits_every_allowed_item_atomically_and_skips_rejected(client):
    create_template(client)
    t1 = submit_task(client, "bp-abort-1")
    t2 = submit_task(client, "bp-abort-2")
    t3 = submit_task(client, "bp-abort-3", priority=90)
    make_succeeded(client, t3["id"])

    result = preview(client, task_ids=[t1["id"], t2["id"], t3["id"]])
    response = confirm(client, result["token"], result["preview_digest"])
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["committed_count"] == 2 and body["skipped_count"] == 1
    committed_ids = {item["task_id"] for item in body["committed"]}
    assert committed_ids == {t1["id"], t2["id"]}
    assert all(item["version"] == 2 for item in body["committed"])
    skipped = body["skipped"][0]
    assert skipped["task_id"] == t3["id"] and "预览时已拒绝" in skipped["reason"]

    details1 = client.get(f"/api/compute/task-details/{t1['id']}").json()
    details2 = client.get(f"/api/compute/task-details/{t2['id']}").json()
    assert details1["status"] == details2["status"] == "cancelled"
    # 每条实际变化都带上同一批次键，串回预览运行
    assert details1["interventions"][0]["batch_key"] == f"protocol:{result['run_id']}"
    assert details2["interventions"][0]["batch_key"] == f"protocol:{result['run_id']}"


def test_abort_mode_rolls_back_everything_when_drift_detected(client):
    create_template(client)
    t1 = submit_task(client, "bp-drift-1")
    t2 = submit_task(client, "bp-drift-2")

    result = preview(client, operation="priority", priority=70, task_ids=[t1["id"], t2["id"]])
    # 预览之后、确认之前，t2 被其他人改动，版本发生漂移
    changed = client.post(
        f"/api/compute/tasks/{t2['id']}/priority",
        json={"actor": "someone-else", "reason": "临时插队", "priority": 95},
    )
    assert changed.status_code == 200

    response = confirm(client, result["token"], result["preview_digest"])
    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "batch_protocol_drift"
    drift_items = error["context"]["drift"]
    assert len(drift_items) == 1 and drift_items[0]["task_id"] == t2["id"]
    assert drift_items[0]["changes"]["version"] == {"preview": 1, "current": 2}
    # 原子性：t1 也不能被提交
    assert client.get(f"/api/compute/task-details/{t1['id']}").json()["version"] == 1
    assert client.get(f"/api/compute/task-details/{t1['id']}").json()["priority"] == 50


def test_accept_partial_mode_skips_drifted_items_and_commits_the_rest(client):
    create_template(client)
    t1 = submit_task(client, "bp-partial-1")
    t2 = submit_task(client, "bp-partial-2")

    result = preview(
        client, operation="priority", priority=70,
        task_ids=[t1["id"], t2["id"]], execution_mode="accept_partial",
    )
    changed = client.post(
        f"/api/compute/tasks/{t2['id']}/priority",
        json={"actor": "someone-else", "reason": "临时插队", "priority": 95},
    )
    assert changed.status_code == 200

    response = confirm(client, result["token"], result["preview_digest"])
    assert response.status_code == 200, response.text
    body = response.json()
    assert [item["task_id"] for item in body["committed"]] == [t1["id"]]
    skipped = body["skipped"][0]
    assert skipped["task_id"] == t2["id"] and "版本已漂移" in skipped["reason"]
    assert client.get(f"/api/compute/task-details/{t1['id']}").json()["priority"] == 70
    assert client.get(f"/api/compute/task-details/{t2['id']}").json()["priority"] == 95
    # 响应同时回传确认时观察到的差异
    assert body["drift"]["items"][0]["task_id"] == t2["id"]


def test_confirmation_token_can_only_be_consumed_once(client):
    create_template(client)
    t1 = submit_task(client, "bp-once-1")
    result = preview(client, task_ids=[t1["id"]])
    first = confirm(client, result["token"], result["preview_digest"])
    assert first.status_code == 200
    second = confirm(client, result["token"], result["preview_digest"])
    assert second.status_code == 409
    assert second.json()["error"]["code"] == "batch_protocol_token_consumed"
    # 重复确认没有产生第二条干预
    details = client.get(f"/api/compute/task-details/{t1['id']}").json()
    assert len(details["interventions"]) == 1


def test_wrong_preview_digest_is_rejected_without_consuming_token(client):
    create_template(client)
    t1 = submit_task(client, "bp-digest-1")
    result = preview(client, task_ids=[t1["id"]])
    bad = confirm(client, result["token"], "0" * 64)
    assert bad.status_code == 409
    assert bad.json()["error"]["code"] == "batch_protocol_digest_mismatch"
    # 凭据仍未消费，持正确摘要仍可确认
    good = confirm(client, result["token"], result["preview_digest"])
    assert good.status_code == 200


def test_confirm_actor_must_match_preview_actor(client):
    create_template(client)
    t1 = submit_task(client, "bp-actor-1")
    result = preview(client, task_ids=[t1["id"]], actor="admin-a")
    response = confirm(client, result["token"], result["preview_digest"], actor="admin-b")
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "batch_protocol_actor_mismatch"
    # 原发起人仍可确认
    assert confirm(client, result["token"], result["preview_digest"], actor="admin-a").status_code == 200


def test_unknown_token_returns_not_found(client):
    create_template(client)
    response = confirm(client, "x" * 43, "a" * 64)
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "batch_protocol_token_invalid"


def test_expired_preview_cannot_be_confirmed_and_reports_expiry(client):
    clock = FrozenClock(datetime(2026, 9, 29, 8, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    t1 = service.submit(
        {
            "template_code": "solver-b", "project_code": "lab-a", "requested_by": "teacher-1",
            "parameters": {"iterations": 1, "mode": "fast"}, "priority": 50,
            "idempotency_key": "bp-expiry-1",
        }
    )
    result = service.preview_batch(
        {"operation": "cancel", "actor": "admin-a", "reason": "过期测试", "task_ids": [t1["id"]],
         "filter": None, "execution_mode": "abort", "ttl_seconds": 30, "priority": None},
    )
    clock.advance(seconds=31)
    with pytest.raises(ConflictError) as exc_info:
        service.confirm_batch({"token": result["token"], "preview_digest": result["preview_digest"], "actor": "admin-a"})
    assert exc_info.value.code == "batch_protocol_expired"
    # 过期响应同样返回确认时观察到的逐项差异（此处任务未变，差异为空）
    assert exc_info.value.context["drift"] == []
    # 过期不消费凭据、不产生修改
    assert service.get_task(t1["id"])["status"] == "queued"
    trace = service.get_protocol_run(result["token"])
    assert trace["status"] == "expired"


def test_expired_preview_reports_drift_when_tasks_changed(client):
    clock = FrozenClock(datetime(2026, 9, 29, 9, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    t1 = service.submit(
        {
            "template_code": "solver-b", "project_code": "lab-a", "requested_by": "teacher-1",
            "parameters": {"iterations": 1, "mode": "fast"}, "priority": 50,
            "idempotency_key": "bp-expiry-drift",
        }
    )
    result = service.preview_batch(
        {"operation": "cancel", "actor": "admin-a", "reason": "过期漂移", "task_ids": [t1["id"]],
         "filter": None, "execution_mode": "abort", "ttl_seconds": 30, "priority": None},
    )
    # 过期窗口内任务又被其他人改动
    service.set_priority(t1["id"], "someone-else", "插队", 99)
    clock.advance(seconds=31)
    with pytest.raises(ConflictError) as exc_info:
        service.confirm_batch({"token": result["token"], "preview_digest": result["preview_digest"], "actor": "admin-a"})
    assert exc_info.value.code == "batch_protocol_expired"
    drift = exc_info.value.context["drift"]
    assert len(drift) == 1 and drift[0]["task_id"] == t1["id"]
    assert drift[0]["changes"]["version"] == {"preview": 1, "current": 2}


def test_audit_trail_links_preview_confirmer_and_per_item_changes(client):
    create_template(client)
    t1 = submit_task(client, "bp-audit-1")
    t2 = submit_task(client, "bp-audit-2", priority=90)
    make_succeeded(client, t2["id"])
    result = preview(client, task_ids=[t1["id"], t2["id"]], actor="admin-a")
    assert confirm(client, result["token"], result["preview_digest"], actor="admin-a").status_code == 200

    run_id = result["run_id"]
    connection = get_connection()
    events = connection.execute(
        "SELECT action,actor_name,outcome,correlation_id FROM audit_events WHERE resource_type='compute_batch_protocol_run' AND resource_id=? ORDER BY id",
        (str(run_id),),
    ).fetchall()
    assert [(row["action"], row["actor_name"], row["outcome"]) for row in events] == [
        ("compute.batch_protocol.preview", "admin-a", "success"),
        ("compute.batch_protocol.confirm", "admin-a", "success"),
    ]
    assert {row["correlation_id"] for row in events} == {f"batch-protocol-{run_id}"}

    interventions = connection.execute(
        "SELECT action,batch_key FROM compute_interventions WHERE task_id=?",
        (t1["id"],),
    ).fetchall()
    assert len(interventions) == 1
    assert interventions[0]["action"] == "cancel" and interventions[0]["batch_key"] == f"protocol:{run_id}"

    # 事后通过凭据可完整解释每一条为什么提交或被跳过
    trace = client.get(f"/api/compute/tasks/batch-protocol/runs/{result['token']}").json()
    assert trace["confirmed_by"] == "admin-a"
    assert trace["committed_count"] == 1 and trace["skipped_count"] == 1
    by_id = {item["task_id"]: item for item in trace["items"]}
    committed = by_id[t1["id"]]
    skipped = by_id[t2["id"]]
    assert committed["outcome"] == "committed" and committed["actual_status"] == "cancelled"
    assert committed["intervention_id"] is not None
    assert skipped["outcome"] == "skipped" and "预览时已拒绝" in skipped["detail"]


def test_denied_confirmation_is_also_audited(client):
    create_template(client)
    t1 = submit_task(client, "bp-denied-1")
    result = preview(client, task_ids=[t1["id"]])
    assert confirm(client, result["token"], result["preview_digest"], actor="admin-b").status_code == 403
    connection = get_connection()
    denied = connection.execute(
        "SELECT outcome,metadata_json FROM audit_events WHERE action='compute.batch_protocol.confirm' AND resource_id=? ORDER BY id DESC LIMIT 1",
        (str(result["run_id"]),),
    ).fetchone()
    assert denied["outcome"] == "denied"
    assert "batch_protocol_actor_mismatch" in denied["metadata_json"]
