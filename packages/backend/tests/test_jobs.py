from datetime import datetime, timedelta
from app.extensions import db
from app.models import BackgroundJob, JobStatus, Reminder
from app.services.jobs import (
    compute_backoff_delay,
    enqueue_job,
    execute_job,
    process_due_jobs,
    register_job_handler,
)


def test_compute_backoff_delay_exponential_with_cap():
    assert compute_backoff_delay(1, base_delay_seconds=10) == 10
    assert compute_backoff_delay(2, base_delay_seconds=10) == 20
    assert compute_backoff_delay(3, base_delay_seconds=10) == 40
    assert (
        compute_backoff_delay(10, base_delay_seconds=30, max_delay_seconds=300) == 300
    )


def test_job_enqueue_idempotency_and_execution(client, auth_header):
    payload = {
        "job_type": "noop",
        "payload": {"task": "verify"},
        "max_attempts": 3,
        "base_delay_seconds": 15,
        "idempotency_key": "idem-noop-1",
    }
    r1 = client.post("/jobs", json=payload, headers=auth_header)
    assert r1.status_code == 201
    job1 = r1.get_json()
    assert job1["status"] == "PENDING"
    assert job1["attempts"] == 0

    # Duplicate idempotency key returns the same job record
    r2 = client.post("/jobs", json=payload, headers=auth_header)
    assert r2.status_code == 201
    assert r2.get_json()["id"] == job1["id"]

    # Run due jobs
    run_res = client.post("/jobs/run", json={"limit": 10}, headers=auth_header)
    assert run_res.status_code == 200
    summary = run_res.get_json()
    assert summary["processed"] == 1
    assert summary["succeeded"] == 1
    assert summary["retried"] == 0
    assert summary["dead_lettered"] == 0

    detail = client.get(f"/jobs/{job1['id']}", headers=auth_header)
    assert detail.status_code == 200
    assert detail.get_json()["status"] == "SUCCEEDED"
    assert detail.get_json()["attempts"] == 1


def test_job_retry_backoff_dead_letter_and_manual_retry(
    app_fixture, client, auth_header
):
    state = {"should_fail": True}

    def flaky_handler(_payload, _job):
        if state["should_fail"]:
            raise RuntimeError("transient upstream timeout")

    register_job_handler("flaky_sync", flaky_handler)

    create_res = client.post(
        "/jobs",
        json={
            "job_type": "flaky_sync",
            "payload": {"account": 42},
            "max_attempts": 2,
            "base_delay_seconds": 60,
        },
        headers=auth_header,
    )
    assert create_res.status_code == 201
    job_id = create_res.get_json()["id"]

    t0 = datetime.utcnow()
    with app_fixture.app_context():
        job = db.session.get(BackgroundJob, job_id)
        execute_job(job, now=t0)
        assert job.status == JobStatus.RETRY_SCHEDULED.value
        assert job.attempts == 1
        assert "transient upstream timeout" in (job.last_error or "")
        assert job.next_run_at == t0 + timedelta(seconds=60)

        # Before backoff window expires, process_due_jobs skips it
        early = process_due_jobs(user_id=job.user_id, now=t0 + timedelta(seconds=30))
        assert early["processed"] == 0

        # Once backoff window elapses, second failure moves job to DEAD_LETTER
        after_backoff = process_due_jobs(
            user_id=job.user_id, now=t0 + timedelta(seconds=61)
        )
        assert after_backoff["processed"] == 1
        assert after_backoff["dead_lettered"] == 1
        db.session.refresh(job)
        assert job.status == JobStatus.DEAD_LETTER.value
        assert job.attempts == 2

    # Monitor endpoint reports unhealthy dead-letter state
    mon = client.get("/jobs/monitor", headers=auth_header)
    assert mon.status_code == 200
    mon_data = mon.get_json()
    assert mon_data["dead_letter_count"] == 1
    assert mon_data["healthy"] is False
    assert len(mon_data["recent_failures"]) == 1

    # Recover upstream and manually retry dead-letter job
    state["should_fail"] = False
    retry_res = client.post(f"/jobs/{job_id}/retry", headers=auth_header)
    assert retry_res.status_code == 200
    assert retry_res.get_json()["status"] == "PENDING"
    assert retry_res.get_json()["attempts"] == 0

    run_res = client.post("/jobs/run", headers=auth_header)
    assert run_res.status_code == 200
    assert run_res.get_json()["succeeded"] == 1

    mon_after = client.get("/jobs/monitor", headers=auth_header).get_json()
    assert mon_after["dead_letter_count"] == 0
    assert mon_after["healthy"] is True

    # Verify Prometheus metrics expose background job counters
    metrics_text = client.get("/metrics").get_data(as_text=True)
    assert "finmind_background_jobs_total" in metrics_text
    assert "finmind_background_job_retries_total" in metrics_text
    assert "finmind_background_job_duration_seconds" in metrics_text


def test_reminder_delivery_handler_retries_on_failure(app_fixture, client, auth_header):
    rem = client.post(
        "/reminders",
        json={
            "message": "Pay water bill",
            "send_at": datetime.utcnow().isoformat(),
            "channel": "whatsapp",
        },
        headers=auth_header,
    )
    assert rem.status_code == 201
    reminder_id = rem.get_json()["id"]

    with app_fixture.app_context():
        reminder = db.session.get(Reminder, reminder_id)
        job = enqueue_job(
            job_type="reminder_delivery",
            payload={"reminder_id": reminder_id},
            user_id=reminder.user_id,
            max_attempts=2,
            base_delay_seconds=10,
        )
        execute_job(job)
        assert job.status == JobStatus.RETRY_SCHEDULED.value
        assert job.attempts == 1
        db.session.refresh(reminder)
        assert reminder.sent is False
