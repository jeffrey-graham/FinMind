from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt_identity, jwt_required

from ..extensions import db
from ..models import BackgroundJob, JobStatus
from ..services.jobs import (
    enqueue_job,
    get_job_health_summary,
    process_due_jobs,
    retry_dead_letter_job,
    serialize_job,
)

bp = Blueprint("jobs", __name__)


@bp.post("")
@jwt_required()
def create_job():
    uid = int(get_jwt_identity())
    data = request.get_json(silent=True) or {}
    job_type = str(data.get("job_type") or "").strip()
    if not job_type:
        return jsonify(error="job_type is required"), 400
    payload = data.get("payload") or {}
    if not isinstance(payload, dict):
        return jsonify(error="payload must be an object"), 400
    try:
        max_attempts = int(data.get("max_attempts", 3))
        base_delay_seconds = int(data.get("base_delay_seconds", 30))
    except (ValueError, TypeError):
        return (
            jsonify(error="max_attempts and base_delay_seconds must be integers"),
            400,
        )
    if max_attempts < 1 or base_delay_seconds < 1:
        return (
            jsonify(error="max_attempts and base_delay_seconds must be >= 1"),
            400,
        )
    idempotency_key = data.get("idempotency_key")
    if idempotency_key is not None:
        idempotency_key = str(idempotency_key).strip() or None

    job = enqueue_job(
        job_type=job_type,
        payload=payload,
        user_id=uid,
        max_attempts=max_attempts,
        base_delay_seconds=base_delay_seconds,
        idempotency_key=idempotency_key,
    )
    return jsonify(serialize_job(job)), 201


@bp.get("")
@jwt_required()
def list_jobs():
    uid = int(get_jwt_identity())
    status = request.args.get("status")
    job_type = request.args.get("job_type")

    query = db.session.query(BackgroundJob).filter_by(user_id=uid)
    if status:
        query = query.filter_by(status=status.upper().strip())
    if job_type:
        query = query.filter_by(job_type=job_type.strip())

    items = query.order_by(
        BackgroundJob.created_at.desc(), BackgroundJob.id.desc()
    ).all()
    return jsonify([serialize_job(j) for j in items]), 200


@bp.get("/monitor")
@jwt_required()
def monitor_jobs():
    uid = int(get_jwt_identity())
    summary = get_job_health_summary(user_id=uid)
    return jsonify(summary), 200


@bp.get("/<int:job_id>")
@jwt_required()
def get_job(job_id: int):
    uid = int(get_jwt_identity())
    job = db.session.get(BackgroundJob, job_id)
    if not job or job.user_id != uid:
        return jsonify(error="not found"), 404
    return jsonify(serialize_job(job)), 200


@bp.post("/run")
@jwt_required()
def run_due_jobs():
    uid = int(get_jwt_identity())
    data = request.get_json(silent=True) or {}
    try:
        limit = int(data.get("limit", 50))
    except (ValueError, TypeError):
        return jsonify(error="limit must be an integer"), 400
    if limit < 1 or limit > 500:
        return jsonify(error="limit must be between 1 and 500"), 400

    result = process_due_jobs(user_id=uid, limit=limit)
    return jsonify(result), 200


@bp.post("/<int:job_id>/retry")
@jwt_required()
def retry_job(job_id: int):
    uid = int(get_jwt_identity())
    job = db.session.get(BackgroundJob, job_id)
    if not job or job.user_id != uid:
        return jsonify(error="not found"), 404
    if job.status not in (
        JobStatus.DEAD_LETTER.value,
        JobStatus.RETRY_SCHEDULED.value,
    ):
        return (
            jsonify(
                error="only DEAD_LETTER or RETRY_SCHEDULED jobs can be manually retried"
            ),
            400,
        )
    updated = retry_dead_letter_job(job)
    return jsonify(serialize_job(updated)), 200
