from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from src.final_common import json_safe
from src.file_router import route_file
from src.batch_loader import run_batch_loader
from src.spark_loader import run_spark_loader
from src.elt_pipeline import process_elt_transformation
from src.mongo_setup import init_mongo
from src.metrics import save_metrics

from src.final_queries import (
    QUERY_DEFINITIONS,
    run_query,
    create_indexes,
    explain_before_after,
)

from src.aggregations import (
    AGGREGATION_DEFINITIONS,
    run_aggregation,
)

from src.materialized_views import (
    refresh_materialized_views,
    get_view,
)

from src.scheduled_jobs import (
    configure_scheduler,
    list_jobs,
    run_job,
)

import os
import time
import uuid


# =========================================================
# FastAPI Application
# =========================================================

app = FastAPI(
    title="Hybrid Big Data Pipeline API",
    version="2.0",
    description="Unified API for the Hybrid Big Data Pipeline - Final Phase",
)


# =========================================================
# Request Models
# =========================================================

class IngestRequest(BaseModel):
    file: str


class QueryRequest(BaseModel):
    params: dict = Field(default_factory=dict)


class JobRequest(BaseModel):
    name: str | None = None


# =========================================================
# Startup
# =========================================================

@app.on_event("startup")
def startup():
    """
    Initialize MongoDB and configure scheduled jobs
    when the API starts.
    """
    init_mongo()
    configure_scheduler()


# =========================================================
# Health
# =========================================================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "hybrid-big-data-pipeline"
    }


# =========================================================
# Ingest
# =========================================================

@app.post("/ingest")
def ingest(req: IngestRequest):

    if not os.path.exists(req.file):
        raise HTTPException(
            status_code=404,
            detail=f"File not found: {req.file}"
        )

    init_mongo()

    # -----------------------------------------------------
    # File Router
    # -----------------------------------------------------

    engine, file_size_mb = route_file(req.file)

    # -----------------------------------------------------
    # Create Run ID
    # -----------------------------------------------------

    run_id = str(uuid.uuid4())

    started = time.time()

    # -----------------------------------------------------
    # Select Loader
    # -----------------------------------------------------

    if engine == "python_batch":
        load_stats = run_batch_loader(
            req.file,
            run_id
        )
    else:
        load_stats = run_spark_loader(
            req.file,
            run_id
        )

    # -----------------------------------------------------
    # ELT Transformation
    # -----------------------------------------------------

    elt_stats = process_elt_transformation(run_id)

    # -----------------------------------------------------
    # Metrics / Report
    # -----------------------------------------------------

    elapsed = time.time() - started

    loaded_raw = load_stats.get(
        "loaded_raw",
        0
    )

    report = {
        "run_id": run_id,

        "file_name": os.path.basename(
            req.file
        ),

        "file_size_mb": round(
            file_size_mb,
            2
        ),

        "engine_used": engine,

        "rows_read": loaded_raw,

        "raw_loaded": loaded_raw,

        "valid_count": elt_stats[
            "count_valid"
        ],

        "corrected_count": elt_stats[
            "count_corrected"
        ],

        "quarantine_count": elt_stats[
            "count_quarantine"
        ],
"inserted_count": elt_stats[
            "count_inserted"
        ],

        "updated_count": elt_stats[
            "count_updated"
        ],

        "unchanged_count": elt_stats[
            "count_unchanged"
        ],

        "elapsed_seconds": round(
            elapsed,
            2
        ),

        "throughput": round(
            loaded_raw / max(
                elapsed,
                0.0001
            ),
            2
        ),

        "error_case_counts": elt_stats[
            "error_case_counts"
        ],

        "consistency_check": elt_stats[
            "consistency_check"
        ]
    }

    # -----------------------------------------------------
    # Save Metrics
    # -----------------------------------------------------

    save_metrics(report)

    return json_safe(report)


# =========================================================
# Indexes
# =========================================================

@app.get("/indexes")
def indexes():

    from pymongo import MongoClient
    from config.settings import (
        MONGO_URI,
        DB_NAME,
        COLLECTION_VALIDATED
    )

    client = MongoClient(MONGO_URI)

    try:
        return json_safe(
            client[
                DB_NAME
            ][
                COLLECTION_VALIDATED
            ].index_information()
        )

    finally:
        client.close()


@app.post("/indexes")
def indexes_create():
    """
    Create the required indexes and
    run the index-related functionality.
    """
    return json_safe(
        create_indexes()
    )


# =========================================================
# Queries
# =========================================================

@app.get("/queries")
def queries():
    """
    Return all available query definitions.
    """
    return json_safe(
        QUERY_DEFINITIONS
    )


@app.get("/queries/{name}")
def query(
    name: str,
    customer_id: str | None = None,
    start: str | None = None,
    end: str | None = None,
    minimum: float = 100000,
    limit: int = 100
):

    if name not in QUERY_DEFINITIONS:
        raise HTTPException(
            status_code=404,
            detail="Unknown query"
        )

    params = {
        "customer_id": customer_id,
        "start": start,
        "end": end,
        "minimum": minimum,
        "limit": limit
    }

    try:

        result = run_query(
            name,
            params
        )

        return json_safe(result)

    except (ValueError, KeyError) as e:

        raise HTTPException(
            status_code=400,
            detail=str(e)
        )


# =========================================================
# Aggregations
# =========================================================

@app.get("/aggregations")
def aggregations():
    """
    Return all available aggregation reports.
    """
    return json_safe(
        AGGREGATION_DEFINITIONS
    )


@app.get("/aggregations/{name}")
def aggregation(name: str):

    if name not in AGGREGATION_DEFINITIONS:
        raise HTTPException(
            status_code=404,
            detail="Unknown aggregation"
        )

    try:

        result = run_aggregation(
            name
        )

        return json_safe(result)

    except (ValueError, KeyError) as e:

        raise HTTPException(
            status_code=400,
            detail=str(e)
        )


# =========================================================
# Materialized Views
# =========================================================

@app.post("/refresh-mv")
def refresh_mv():
    """
    Refresh materialized views using
    incremental refresh.
    """
    return json_safe(
        refresh_materialized_views(
            incremental=True
        )
    )


@app.get("/views/{name}")
def view(name: str):

    try:

        return json_safe(
            get_view(name)
        )

    except KeyError:

        raise HTTPException(
            status_code=404,
            detail="Unknown materialized view"
        )
# =========================================================
# Scheduled Jobs
# =========================================================

@app.get("/jobs")
def jobs():
    """
    Return the scheduled jobs.

    This endpoint is GET because the
    official final-project requirements
    specify GET /jobs.
    """
    return json_safe({
        "scheduled_jobs": list_jobs()
    })


@app.post("/jobs/{name}/run")
def jobs_run(name: str):
    """
    Manually execute a scheduled job.
    """

    try:

        return json_safe(
            run_job(name)
        )

    except KeyError:

        raise HTTPException(
            status_code=404,
            detail="Unknown job"
        )


# =========================================================
# Explain
# =========================================================

@app.get("/explain/{name}")
def explain(
    name: str,
    customer_id: str | None = None,
    start: str | None = None,
    end: str | None = None,
    city: str | None = None
):

    params = {
        "customer_id": customer_id,
        "start": start,
        "end": end,
        "city": city
    }

    try:

        result = explain_before_after(
            name,
            params
        )

        return json_safe(result)

    except (ValueError, KeyError) as e:

        raise HTTPException(
            status_code=400,
            detail=str(e)
        )