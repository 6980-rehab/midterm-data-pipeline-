import sys
import os
import time
import hashlib
import json
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MONGO_URI, DB_NAME, COLLECTION_RAW, COLLECTION_VALIDATED, COLLECTION_QUARANTINE, ELT_CHUNK_SIZE
from src.quality_rules import validate_and_clean_record

def process_elt_transformation(run_id, batch_chunk_size=ELT_CHUNK_SIZE):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    raw_col = db[COLLECTION_RAW]
    valid_col = db[COLLECTION_VALIDATED]
    quarantine_col = db[COLLECTION_QUARANTINE]

    start_time = time.time()
    
    total_raw_in_run = raw_col.count_documents({"run_id": run_id})
    print(f"\n[ELT Pipeline] Processing {total_raw_in_run} raw records for run_id: {run_id} ...")

    raw_cursor = raw_col.find({"run_id": run_id}).batch_size(batch_chunk_size)
    
    valid_bulk_ops = []
    quarantine_docs = []
    
    count_valid = 0
    count_corrected = 0
    count_quarantine = 0
    total_inserted = 0
    total_updated = 0
    error_cases_count = {}

    for doc in raw_cursor:
        raw_data = doc.get("raw_record", {})
        result = validate_and_clean_record(raw_data)
        status = result["status"]
        data = result["data"]

        if status in ["valid", "corrected"]:
            if status == "valid":
                count_valid += 1
            else:
                count_corrected += 1
            
            # Final-phase fields: parsed items for aggregations and a stable content hash.
            try:
                parsed_items = json.loads(data.get("items_json", "[]")) if isinstance(data.get("items_json"), str) else data.get("items_json", [])
            except Exception:
                parsed_items = []
            data["items_parsed"] = parsed_items if isinstance(parsed_items, list) else [parsed_items]
            stable_payload = json.dumps({k: v for k, v in data.items() if k not in {"quality_status", "corrections"}}, ensure_ascii=False, sort_keys=True, default=str)
            content_version = hashlib.sha256(stable_payload.encode("utf-8")).hexdigest()
            operation_time = datetime.now(timezone.utc).isoformat()
            existing = valid_col.find_one({"order_id": data["order_id"]}, {"content_version": 1, "updated_at": 1})
            if existing and existing.get("content_version") == content_version:
                data["updated_at"] = existing.get("updated_at", operation_time)
            else:
                data["updated_at"] = operation_time
            data["content_version"] = content_version
            valid_bulk_ops.append(
                UpdateOne(
                    {"order_id": data["order_id"]},
                    {"$set": data, "$setOnInsert": {"created_at": operation_time}},
                    upsert=True
                )
            )
        else:
            count_quarantine += 1
            data["run_id"] = run_id
            quarantine_docs.append(data)
            for err in data.get("error_codes", []):
                error_cases_count[err] = error_cases_count.get(err, 0) + 1

        if len(valid_bulk_ops) >= batch_chunk_size:
            bulk_res = valid_col.bulk_write(valid_bulk_ops, ordered=False)
            total_inserted += bulk_res.upserted_count
            total_updated += bulk_res.modified_count
            valid_bulk_ops = []

        if len(quarantine_docs) >= batch_chunk_size:
            quarantine_col.insert_many(quarantine_docs, ordered=False)
            quarantine_docs = []

    if valid_bulk_ops:
        bulk_res = valid_col.bulk_write(valid_bulk_ops, ordered=False)
        total_inserted += bulk_res.upserted_count
        total_updated += bulk_res.modified_count

    if quarantine_docs:
        quarantine_col.insert_many(quarantine_docs, ordered=False)

    total_raw_processed = count_valid + count_corrected + count_quarantine
    total_unchanged = (count_valid + count_corrected) - (total_inserted + total_updated)
    duration = time.time() - start_time

    # فحص معادلة الاتساق الإلزامية (البند 6.11)
    consistency_passed = (total_raw_processed == total_raw_in_run)
    print(f"\n[ELT Pipeline] Finished {total_raw_processed} records in {duration:.2f}s")
    print(f"Consistency Check: {'PASSED (OK)' if consistency_passed else 'FAILED'}")
    print(f"  Valid: {count_valid} | Corrected: {count_corrected} | Quarantine: {count_quarantine}")
    print(f"  Upsert Metrics -> Inserted: {total_inserted}, Updated: {total_updated}, Unchanged: {total_unchanged}")

    client.close()

    return {
        "run_id": run_id,
        "processed_raw": total_raw_processed,
        "count_valid": count_valid,
        "count_corrected": count_corrected,
        "count_quarantine": count_quarantine,
        "count_inserted": total_inserted,
        "count_updated": total_updated,
        "count_unchanged": total_unchanged,
        "error_case_counts": error_cases_count,
        "transformation_seconds": duration,
        "consistency_check": consistency_passed
    }