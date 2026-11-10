from datetime import datetime, timedelta
import json
import logging
import time
from typing import Any, Callable

from ..extensions import db
from ..models import BackgroundJob, JobStatus, Reminder
from ..observability import track_background_job, track_reminder_event
from .reminders import send_reminder

logger = logging.getLogger("finmind.jobs")

JobHandler = Callable[[dict[str, Any], BackgroundJob], None]
_HANDLERS: dict[str, JobHandler] = {}


def register_job_handler(job_type: str, handler: JobHandler) -> None:
    _HANDLERS[job_type] = handler


def compute_backoff_delay(
    attempt: int, base_delay_seconds: int = 30, max_delay_seconds: int = 3600
) -> int:
    exponent = max(0, int(attempt) - 1)
    delay = int(base_delay_seconds) * (2**exponent)
    return min(int(max_delay_seconds), max(1, delay))


def _handle_reminder_delivery(payload: dict[str, Any], job: BackgroundJob) -> None:
    reminder_id = payload.get("reminder_id")
    if reminder_id is None:
        raise ValueError("reminder_id is required in payload")
    reminder = db.session.get(Reminder, int(reminder_id))
    if not reminder:
        raise ValueError(f"reminder {reminder_id} not found")
    if reminder.sent:
        return
    delivered = send_reminder(reminder)
    if not delivered:
        track_reminder_event(
            event="delivery_failed", channel=reminder.channel, status="error"
        )
        raise RuntimeError(
            f"delivery failed for reminder {reminder.id} on {reminder.channel}"
        )
    reminder.sent = True
    track_reminder_event(event="sent", channel=reminder.channel)


def _handle_noop(_payload: dict[str, Any], _job: BackgroundJob) -> None:
    return


register_job_handler("reminder_delivery", _handle_reminder_delivery)
register_job_handler("noop", _handle_noop)


def serialize_job(job: BackgroundJob) -> dict[str, Any]:
    parsed_payload: Any = {}
    if job.payload:
        try:
            parsed_payload = json.loads(job.payload)
        except Exception:
            parsed_payload = job.payload
    return {
        "id": job.id,
        "user_id": job.user_id,
        "job_type": job.job_type,
        "payload": parsed_payload,
        "status": job.status,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "base_delay_seconds": job.base_delay_seconds,
        "next_run_at": job.next_run_at.isoformat() if job.next_run_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "last_error": job.last_error,
        "idempotency_key": job.idempotency_key,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
    }


def enqueue_job(
    *,
    job_type: str,
    payload: dict[str, Any] | None = None,
    user_id: int | None = None,
    max_attempts: int = 3,
    base_delay_seconds: int = 30,
    idempotency_key: str | None = None,
    run_at: datetime | None = None,
) -> BackgroundJob:
    if not job_type or not str(job_type).strip():
        raise ValueError("job_type is required")
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    if base_delay_seconds < 1:
        raise ValueError("base_delay_seconds must be >= 1")

    if idempotency_key:
        existing = (
            db.session.query(BackgroundJob)
            .filter_by(idempotency_key=idempotency_key)
            .first()
        )
        if existing:
            return existing

    now = datetime.utcnow()
    job = BackgroundJob(
        user_id=user_id,
        job_type=str(job_type).strip(),
        payload=json.dumps(payload or {}, separators=(",", ":")),
        status=JobStatus.PENDING.value,
        attempts=0,
        max_attempts=int(max_attempts),
        base_delay_seconds=int(base_delay_seconds),
        next_run_at=run_at or now,
        idempotency_key=idempotency_key,
        created_at=now,
        updated_at=now,
    )
    db.session.add(job)
    db.session.commit()
    track_background_job(job_type=job.job_type, status="ENQUEUED")
    return job


def execute_job(job: BackgroundJob, now: datetime | None = None) -> BackgroundJob:
    current_time = now or datetime.utcnow()
    start_perf = time.perf_counter()
    job.status = JobStatus.RUNNING.value
    job.started_at = current_time
    job.attempts = (job.attempts or 0) + 1
    job.updated_at = current_time
    db.session.commit()

    payload_dict: dict[str, Any] = {}
    if job.payload:
        try:
            payload_dict = json.loads(job.payload)
        except Exception:
            payload_dict = {}

    handler = _HANDLERS.get(job.job_type)
    try:
        if handler is None:
            raise KeyError(f"unregistered job_type: {job.job_type}")
        handler(payload_dict, job)
        elapsed = time.perf_counter() - start_perf
        job.status = JobStatus.SUCCEEDED.value
        job.completed_at = current_time
        job.last_error = None
        job.updated_at = current_time
        db.session.commit()
        track_background_job(
            job_type=job.job_type,
            status=JobStatus.SUCCEEDED.value,
            duration_seconds=elapsed,
        )
    except Exception as exc:
        elapsed = time.perf_counter() - start_perf
        err_msg = str(exc)[:1000]
        job.last_error = err_msg
        job.updated_at = current_time
        if job.attempts < job.max_attempts:
            delay = compute_backoff_delay(job.attempts, job.base_delay_seconds)
            job.status = JobStatus.RETRY_SCHEDULED.value
            job.next_run_at = current_time + timedelta(seconds=delay)
            db.session.commit()
            track_background_job(
                job_type=job.job_type,
                status=JobStatus.RETRY_SCHEDULED.value,
                duration_seconds=elapsed,
                retried=True,
            )
            logger.warning(
                "Background job id=%s type=%s failed (attempt %s/%s); retry in %ss: %s",
                job.id,
                job.job_type,
                job.attempts,
                job.max_attempts,
                delay,
                err_msg,
            )
        else:
            job.status = JobStatus.DEAD_LETTER.value
            job.completed_at = current_time
            db.session.commit()
            track_background_job(
                job_type=job.job_type,
                status=JobStatus.DEAD_LETTER.value,
                duration_seconds=elapsed,
            )
            logger.error(
                "Background job id=%s type=%s DEAD_LETTER after %s attempts: %s",
                job.id,
                job.job_type,
                job.attempts,
                err_msg,
            )
    return job


def process_due_jobs(
    *,
    user_id: int | None = None,
    limit: int = 50,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_time = now or datetime.utcnow()
    query = db.session.query(BackgroundJob).filter(
        BackgroundJob.status.in_(
            [JobStatus.PENDING.value, JobStatus.RETRY_SCHEDULED.value]
        ),
        BackgroundJob.next_run_at <= current_time,
    )
    if user_id is not None:
        query = query.filter(BackgroundJob.user_id == user_id)

    due_jobs = (
        query.order_by(BackgroundJob.next_run_at.asc(), BackgroundJob.id.asc())
        .limit(limit)
        .all()
    )
    succeeded = 0
    retried = 0
    dead_lettered = 0
    results = []
    for job in due_jobs:
        updated = execute_job(job, now=current_time)
        if updated.status == JobStatus.SUCCEEDED.value:
            succeeded += 1
        elif updated.status == JobStatus.RETRY_SCHEDULED.value:
            retried += 1
        elif updated.status == JobStatus.DEAD_LETTER.value:
            dead_lettered += 1
        results.append(serialize_job(updated))

    return {
        "processed": len(due_jobs),
        "succeeded": succeeded,
        "retried": retried,
        "dead_lettered": dead_lettered,
        "jobs": results,
    }


def retry_dead_letter_job(
    job: BackgroundJob, now: datetime | None = None
) -> BackgroundJob:
    current_time = now or datetime.utcnow()
    job.status = JobStatus.PENDING.value
    job.attempts = 0
    job.last_error = None
    job.completed_at = None
    job.next_run_at = current_time
    job.updated_at = current_time
    db.session.commit()
    track_background_job(job_type=job.job_type, status="MANUAL_RETRY", retried=True)
    return job


def get_job_health_summary(user_id: int | None = None) -> dict[str, Any]:
    query = db.session.query(BackgroundJob)
    if user_id is not None:
        query = query.filter(BackgroundJob.user_id == user_id)
    jobs = query.order_by(
        BackgroundJob.updated_at.desc(), BackgroundJob.id.desc()
    ).all()

    counts = {s.value: 0 for s in JobStatus}
    total_attempts = 0
    for j in jobs:
        counts[j.status] = counts.get(j.status, 0) + 1
        total_attempts += j.attempts or 0

    total = len(jobs)
    dead_letter_count = counts.get(JobStatus.DEAD_LETTER.value, 0)
    retried_jobs = sum(1 for j in jobs if (j.attempts or 0) > 1)
    retry_rate = round(retried_jobs / total, 4) if total > 0 else 0.0
    failure_rate = round(dead_letter_count / total, 4) if total > 0 else 0.0

    recent_failures = [
        serialize_job(j)
        for j in jobs
        if j.status in (JobStatus.DEAD_LETTER.value, JobStatus.RETRY_SCHEDULED.value)
    ][:10]

    return {
        "total_jobs": total,
        "total_attempts": total_attempts,
        "counts_by_status": counts,
        "dead_letter_count": dead_letter_count,
        "retry_rate": retry_rate,
        "failure_rate": failure_rate,
        "healthy": dead_letter_count == 0,
        "recent_failures": recent_failures,
    }
