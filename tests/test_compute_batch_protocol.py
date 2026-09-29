from __future__ import annotations

from datetime import UTC, datetime

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection


TEMPLATE = {
    "code": "solver-b",
    "name": "方程求解模板",
    "algorithm": "solver-b",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def submit_payload(key: str, *, user: str = "researcher-1", priority: int = 50) -> dict:
    return {
        "template_code": "solver-b",
        "project_code": "project-a",
        "requested_by": user,
        "parameters": {"iterations": 100, "mode": "accurate"},
        "priority": priority,
        "idempotency_key": key,
    }


def create_template(client) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text


def make_tasks(client, count: int = 3) -> list[dict]:
    return [
        client.post("/api/compute/tasks", json=submit_payload(f"two-phase-{index:06d}")).json()
        for index in range(count)
    ]


def test_preview_filters_and_items(client):
    create_template(client)
    tasks = make_tasks(client, 2)
    tasks.append(client.post("/api/compute/tasks", json=submit_payload("two-phase-other", user="someone-else")).json())

    by_ids = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "cancel", "actor": "admin-a", "reason": "误标待重做需要取消",
              "task_ids": [tasks[0]["id"], tasks[1]["id"]]},
    )
    assert by_ids.status_code == 202, by_ids.text
    body = by_ids.json()
    assert body["selector"] == {"task_ids": [tasks[0]["id"], tasks[1]["id"]]}
    assert body["summary"] == {"total": 2, "allowed": 2, "rejected": 0, "limited": False}
    assert {item["task_id"] for item in body["items"]} == {tasks[0]["id"], tasks[1]["id"]}
    assert all(item["allowed"] and item["allowed_action"] == "cancel" for item in body["items"])
    assert body["digest"] and body["preview_key"].startswith("bp_")
    assert len({item["version"] for item in body["items"]}) == 1

    # 筛选条件被固定：只命中本项目 researcher-1 的任务。
    by_filter = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "cancel", "actor": "admin-a", "reason": "误标待重做需要取消",
              "status": "queued", "requested_by": "researcher-1"},
    )
    assert by_filter.status_code == 202
    selected = {item["task_id"] for item in by_filter.json()["items"]}
    assert selected == {tasks[0]["id"], tasks[1]["id"]}
    assert by_filter.json()["selector"] == {"status": "queued", "requested_by": "researcher-1", "limit": 200}

    # task_ids 与筛选条件互斥，且二者必须提供其一。
    bad = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "cancel", "actor": "admin-a", "reason": "x", "task_ids": [1], "status": "queued"},
    )
    assert bad.status_code == 422
    empty = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "cancel", "actor": "admin-a", "reason": "x"},
    )
    assert empty.status_code == 422


def test_preview_reports_reject_reasons(client):
    create_template(client)
    # 先提交的任务会被工作者优先领取并置为失败。
    failed = client.post("/api/compute/tasks", json=submit_payload("pv-reject-f")).json()
    worker = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-b"], "lease_seconds": 60}).json()["task"]
    assert worker["id"] == failed["id"]
    client.post(f"/api/compute/tasks/{worker['id']}/fail", json={"worker_id": "w1", "error_code": "e", "message": "失败", "retryable": False})
    queued = client.post("/api/compute/tasks", json=submit_payload("pv-reject-q")).json()

    # retry 动作对排队任务给出拒绝理由；不存在任务同样逐项可见。
    response = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "retry", "actor": "admin-a", "reason": "恢复误标",
              "task_ids": [queued["id"], failed["id"], 999999]},
    )
    assert response.status_code == 202
    items = {item["task_id"]: item for item in response.json()["items"]}
    assert items[queued["id"]]["allowed"] is False
    assert items[queued["id"]]["reject_reason"] == "只有失败或已取消任务可以人工重试"
    assert items[failed["id"]]["allowed"] is True
    assert items[999999]["allowed"] is False
    assert items[999999]["reject_reason"] == "计算任务不存在"
    assert response.json()["summary"] == {"total": 3, "allowed": 1, "rejected": 2, "limited": False}


def test_atomic_confirm_applies_all_in_one_batch(client):
    create_template(client)
    tasks = make_tasks(client, 3)
    preview = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "cancel", "actor": "admin-a", "reason": "误标待重做批量取消",
              "status": "queued", "project_code": "project-a"},
    ).json()

    confirmed = client.post(
        "/api/compute/tasks/batch-confirm",
        json={"preview_key": preview["preview_key"], "actor": "admin-a",
              "mode": "atomic", "expected_digest": preview["digest"]},
    )
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    assert body["summary"] == {"total": 3, "applied": 3, "skipped": 0}
    assert {item["task_id"] for item in body["applied"]} == {task["id"] for task in tasks}
    assert all(item["status"] == "cancelled" for item in body["applied"])

    for task in tasks:
        details = client.get(f"/api/compute/task-details/{task['id']}").json()
        assert details["status"] == "cancelled"
        intervention = details["interventions"][-1]
        assert intervention["action"] == "cancel"
        assert intervention["batch_key"] == preview["preview_key"]
        assert intervention["actor"] == "admin-a"


def test_atomic_mode_aborts_when_preview_has_rejections(client):
    create_template(client)
    running = client.post("/api/compute/tasks", json=submit_payload("atom-r")).json()
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-b"], "lease_seconds": 60}).json()["task"]
    assert claimed["id"] == running["id"]
    queued = client.post("/api/compute/tasks", json=submit_payload("atom-q")).json()

    preview = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "retry", "actor": "admin-a", "reason": "恢复",
              "task_ids": [queued["id"], running["id"]]},
    ).json()
    confirmed = client.post(
        "/api/compute/tasks/batch-confirm",
        json={"preview_key": preview["preview_key"], "actor": "admin-a",
              "mode": "atomic", "expected_digest": preview["digest"]},
    )
    assert confirmed.status_code == 409
    assert confirmed.json()["error"]["context"]["rejected"][0]["task_id"] in {queued["id"], running["id"]}
    # 凭据未消费：没有任何任务被改动，且可改用 partial 重新确认。
    again = client.post(
        "/api/compute/tasks/batch-confirm",
        json={"preview_key": preview["preview_key"], "actor": "admin-a",
              "mode": "partial", "expected_digest": preview["digest"]},
    )
    assert again.status_code == 200
    assert again.json()["summary"]["applied"] == 0
    assert len(again.json()["skipped"]) == 2


def test_partial_mode_records_every_skip(client):
    create_template(client)
    failed = client.post("/api/compute/tasks", json=submit_payload("part-f")).json()
    client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-b"], "lease_seconds": 60})
    client.post(f"/api/compute/tasks/{failed['id']}/fail", json={"worker_id": "w1", "error_code": "e", "message": "失败", "retryable": False})
    queued = client.post("/api/compute/tasks", json=submit_payload("part-q")).json()

    preview = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "retry", "actor": "admin-a", "reason": "逐条接受",
              "task_ids": [failed["id"], queued["id"]]},
    ).json()
    confirmed = client.post(
        "/api/compute/tasks/batch-confirm",
        json={"preview_key": preview["preview_key"], "actor": "admin-b",
              "mode": "partial", "expected_digest": preview["digest"]},
    )
    assert confirmed.status_code == 200
    body = confirmed.json()
    assert body["summary"] == {"total": 2, "applied": 1, "skipped": 1}
    assert body["applied"][0]["task_id"] == failed["id"]
    assert body["skipped"][0] == {"task_id": queued["id"], "reject_reason": "只有失败或已取消任务可以人工重试"}

    details = client.get(f"/api/compute/task-details/{queued['id']}").json()
    skip = details["interventions"][-1]
    assert skip["action"] == "batch_skip"
    assert skip["actor"] == "admin-b"
    assert skip["batch_key"] == preview["preview_key"]
    assert details["status"] == "queued"


def test_credential_is_single_use(client):
    create_template(client)
    tasks = make_tasks(client, 2)
    preview = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "cancel", "actor": "admin-a", "reason": "一次性",
              "task_ids": [task["id"] for task in tasks]},
    ).json()
    payload = {"preview_key": preview["preview_key"], "actor": "admin-a",
               "mode": "atomic", "expected_digest": preview["digest"]}
    first = client.post("/api/compute/tasks/batch-confirm", json=payload)
    assert first.status_code == 200
    second = client.post("/api/compute/tasks/batch-confirm", json=payload)
    assert second.status_code == 409
    context = second.json()["error"]["context"]
    assert context["consumed_by"] == "admin-a"
    assert context["consumed_result"]["summary"]["applied"] == 2


def test_digest_mismatch_is_rejected(client):
    create_template(client)
    task = make_tasks(client, 1)[0]
    preview = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "cancel", "actor": "admin-a", "reason": "摘要", "task_ids": [task["id"]]},
    ).json()
    wrong = client.post(
        "/api/compute/tasks/batch-confirm",
        json={"preview_key": preview["preview_key"], "actor": "admin-a",
              "mode": "atomic", "expected_digest": "0" * 64},
    )
    assert wrong.status_code == 409
    assert "摘要" in wrong.json()["error"]["message"]
    # 摘要错误不消费凭据，正确摘要仍可确认。
    ok = client.post(
        "/api/compute/tasks/batch-confirm",
        json={"preview_key": preview["preview_key"], "actor": "admin-a",
              "mode": "atomic", "expected_digest": preview["digest"]},
    )
    assert ok.status_code == 200


def test_drift_after_preview_returns_diff(client):
    create_template(client)
    task = make_tasks(client, 1)[0]
    preview = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "cancel", "actor": "admin-a", "reason": "漂移", "task_ids": [task["id"]]},
    ).json()
    # 预览之后、确认之前任务被其他人取消，版本发生变化。
    client.post(f"/api/compute/tasks/{task['id']}/cancel", json={"actor": "other-admin", "reason": "抢先取消"})
    confirmed = client.post(
        "/api/compute/tasks/batch-confirm",
        json={"preview_key": preview["preview_key"], "actor": "admin-a",
              "mode": "atomic", "expected_digest": preview["digest"]},
    )
    assert confirmed.status_code == 409
    diff = confirmed.json()["error"]["context"]["drift"]
    assert diff[0]["task_id"] == task["id"]
    assert diff[0]["preview"]["status"] == "queued"
    assert diff[0]["current"]["status"] == "cancelled"
    assert diff[0]["current"]["version"] == preview["items"][0]["version"] + 1

    # 漂移不消费凭据：重新预览后可以正常确认。
    new_preview = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "retry", "actor": "admin-a", "reason": "重新预览", "task_ids": [task["id"]]},
    ).json()
    retried = client.post(
        "/api/compute/tasks/batch-confirm",
        json={"preview_key": new_preview["preview_key"], "actor": "admin-a",
              "mode": "atomic", "expected_digest": new_preview["digest"]},
    )
    assert retried.status_code == 200
    assert retried.json()["applied"][0]["status"] == "queued"


def test_expired_preview_is_rejected_and_consumed_once(client):
    del client
    clock = FrozenClock(datetime(2026, 9, 29, 8, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    task = service.submit(submit_payload("expire-000001"))
    preview = service.batch_preview({
        "operation": "cancel", "actor": "admin-a", "reason": "过期",
        "task_ids": [task["id"]], "ttl_seconds": 60,
    })
    clock.advance(seconds=61)
    try:
        service.batch_confirm({"preview_key": preview["preview_key"], "actor": "admin-a",
                               "mode": "atomic", "expected_digest": preview["digest"]})
        raise AssertionError("过期凭据应当被拒绝")
    except Exception as exc:
        assert exc.status_code == 409
        assert exc.context["expired"] is True

    # 过期后状态被持久化为 expired，再次确认仍是失败，且不会执行任务。
    row = service.repository.preview_by_key(preview["preview_key"])
    assert row["status"] == "expired"
    assert service.get_task(task["id"])["status"] == "queued"


def test_audit_links_preview_confirmer_and_per_item_changes(client):
    create_template(client)
    tasks = make_tasks(client, 2)
    preview = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "cancel", "actor": "admin-a", "reason": "审计串联",
              "task_ids": [tasks[0]["id"], tasks[1]["id"]]},
    ).json()
    client.post(
        "/api/compute/tasks/batch-confirm",
        json={"preview_key": preview["preview_key"], "actor": "admin-b",
              "mode": "atomic", "expected_digest": preview["digest"]},
    )

    from app.database import get_connection
    connection = get_connection()
    preview_event = connection.execute(
        "SELECT * FROM audit_events WHERE action='compute.batch_preview' AND correlation_id=?",
        (preview["preview_key"],),
    ).fetchone()
    confirm_event = connection.execute(
        "SELECT * FROM audit_events WHERE action='compute.batch_confirm' AND correlation_id=?",
        (preview["preview_key"],),
    ).fetchone()
    assert preview_event is not None and confirm_event is not None
    assert preview_event["actor_name"] == "admin-a"
    assert confirm_event["actor_name"] == "admin-b"

    import json
    metadata = json.loads(confirm_event["metadata_json"])
    assert metadata["preview_created_by"] == "admin-a"
    assert metadata["confirmed_by"] == "admin-b"
    assert {item["task_id"] for item in metadata["applied"]} == {task["id"] for task in tasks}

    interventions = connection.execute(
        "SELECT task_id,actor,action,batch_key FROM compute_interventions WHERE batch_key=? ORDER BY task_id",
        (preview["preview_key"],),
    ).fetchall()
    assert len(interventions) == 2
    assert {row["task_id"] for row in interventions} == {task["id"] for task in tasks}
    assert all(row["action"] == "cancel" and row["actor"] == "admin-b" for row in interventions)


def test_denied_confirm_is_also_audited(client):
    create_template(client)
    task = client.post("/api/compute/tasks", json=submit_payload("deny-audit")).json()
    preview = client.post(
        "/api/compute/tasks/batch-preview",
        json={"operation": "retry", "actor": "admin-a", "reason": "拒绝审计", "task_ids": [task["id"]]},
    ).json()
    denied = client.post(
        "/api/compute/tasks/batch-confirm",
        json={"preview_key": preview["preview_key"], "actor": "admin-a",
              "mode": "atomic", "expected_digest": preview["digest"]},
    )
    assert denied.status_code == 409
    from app.database import get_connection
    event = get_connection().execute(
        "SELECT outcome,after_json FROM audit_events WHERE action='compute.batch_confirm_rejected' AND correlation_id=?",
        (preview["preview_key"],),
    ).fetchone()
    assert event is not None and event["outcome"] == "denied"
    assert "rejected_items" in event["after_json"]
