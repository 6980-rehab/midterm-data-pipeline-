import time
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from config.settings import JOB_DAILY_SALES_HOUR, JOB_TOP_PRODUCTS_HOUR, MONGO_URI, DB_NAME
from src.materialized_views import refresh_materialized_views
from pymongo import MongoClient

scheduler = BackgroundScheduler()


def run_job(name):
    started = datetime.utcnow()
    status = "success"
    error = None
    try:
        if name in {"refresh_materialized_views", "daily_sales_summary", "top_products_summary"}:
            result = refresh_materialized_views(incremental=True)
        else:
            raise KeyError(f"Unknown scheduled job: {name}")
    except Exception as exc:
        status = "failed"
        error = str(exc)
        result = None
    ended = datetime.utcnow()
    record = {"job": name, "started_at": started.isoformat(), "ended_at": ended.isoformat(), "status": status, "result": result, "error": error}
    print(f"[JOB] {name}: {status} | {started.isoformat()} -> {ended.isoformat()}")
    client = MongoClient(MONGO_URI)
    try:
        client[DB_NAME]["scheduled_job_runs"].insert_one(record)
    finally:
        client.close()
    return record


def configure_scheduler():
    if not scheduler.running:
        scheduler.add_job(lambda: run_job("daily_sales_summary"), "cron", hour=JOB_DAILY_SALES_HOUR, minute=0, id="daily_sales_summary_job", replace_existing=True)
        scheduler.add_job(lambda: run_job("top_products_summary"), "cron", hour=JOB_TOP_PRODUCTS_HOUR, minute=30, id="top_products_summary_job", replace_existing=True)
        scheduler.start()
    return {"jobs": list_jobs()}


def list_jobs():
    return [
        {"name": "daily_sales_summary", "schedule": f"daily at {JOB_DAILY_SALES_HOUR:02d}:00", "function": "incremental materialized view refresh"},
        {"name": "top_products_summary", "schedule": f"daily at {JOB_TOP_PRODUCTS_HOUR:02d}:30", "function": "incremental materialized view refresh"},
    ]
