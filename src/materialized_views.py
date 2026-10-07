from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from config.settings import MONGO_URI, DB_NAME, COLLECTION_VALIDATED
from src.final_common import money_expr, parse_items, json_safe

VIEW_DAILY = "daily_sales_summary"
VIEW_PRODUCTS = "top_products_summary"
META_COLLECTION = "final_pipeline_metadata"


def _client():
    c = MongoClient(MONGO_URI)
    return c, c[DB_NAME]


def refresh_materialized_views(incremental=True):
    client, db = _client()
    try:
        source = db[COLLECTION_VALIDATED]
        now = datetime.now(timezone.utc).isoformat()
        meta = db[META_COLLECTION]
        state = meta.find_one({"_id": "materialized_views"}) or {"_id": "materialized_views", "watermark": None}
        watermark = state.get("watermark")
        # The midterm records may not contain updated_at, so first refresh can build from all records.
        match = {}
        if incremental and watermark:
            match = {"updated_at": {"$gt": watermark}}

        daily = db[VIEW_DAILY]
        products = db[VIEW_PRODUCTS]
        if not incremental or not watermark:
            daily.drop()
            products.drop()
            daily_docs = list(source.aggregate([
                {"$group": {"_id": "$order_date", "orders": {"$sum": 1}, "sales": {"$sum": {"$convert": {"input": "$total_amount", "to": "double", "onError": 0, "onNull": 0}}}}},
                {"$sort": {"_id": 1}},
            ]))
            products_docs = _rebuild_products(source)
            if daily_docs:
                daily.insert_many(daily_docs)
            if products_docs:
                products.insert_many(products_docs)
            processed = source.count_documents({})
        else:
            changed = list(source.find(match, {"order_id": 1, "order_date": 1, "total_amount": 1, "items_json": 1}))
            # Recalculate only affected days/products. This is incremental at the source-delta level.
            days = sorted({d.get("order_date") for d in changed if d.get("order_date")})
            if days:
                for day in days:
                    doc = next(source.aggregate([
                        {"$match": {"order_date": day}},
                        {"$group": {
                            "_id": day,
                            "orders": {"$sum": 1},
                            "sales": {"$sum": {"$convert": {"input": "$total_amount", "to": "double", "onError": 0, "onNull": 0}}}
                        }}
                    ]), None)
                    daily.replace_one({"_id": day}, doc or {"_id": day, "orders": 0, "sales": 0}, upsert=True)
            affected_products = set()
            for d in changed:
                for item in parse_items(d.get("items_json")):
                    if isinstance(item, dict) and item.get("item_id") is not None:
                        affected_products.add(item.get("item_id"))
            if affected_products:
                rebuilt = _rebuild_products(source, only_ids=affected_products)
                for doc in rebuilt:
                    products.replace_one({"_id": doc["_id"]}, doc, upsert=True)
            processed = len(changed)

        meta.update_one({"_id": "materialized_views"}, {"$set": {"watermark": now, "last_refresh_at": now, "incremental": bool(incremental), "processed_delta": processed}}, upsert=True)
        return {"status": "success", "incremental": bool(incremental), "processed_delta": processed, "updated_at": now, "views": [VIEW_DAILY, VIEW_PRODUCTS]}
    finally:
        client.close()


def _rebuild_products(source, only_ids=None):
    docs = []
    # Build in Python so the feature works with the existing items_json strings and heterogeneous training data.
    totals = {}
    projection = {"items_json": 1}
    for d in source.find({}, projection):
        for item in parse_items(d.get("items_json")):
            if not isinstance(item, dict):
                continue
            pid = item.get("item_id")
            if pid is None or (only_ids is not None and pid not in only_ids):
                continue
            try:
                qty = float(item.get("quantity", 1) or 1)
            except Exception:
                qty = 1.0
            try:
                price = float(str(item.get("price", 0)).replace(",", "") or 0)
            except Exception:
                price = 0.0
            row = totals.setdefault(pid, {"_id": pid, "quantity": 0.0, "sales": 0.0})
            row["quantity"] += qty
            row["sales"] += qty * price
    return sorted(totals.values(), key=lambda x: x["sales"], reverse=True)[:100]


def get_view(name):
    client, db = _client()
    try:
        if name not in {VIEW_DAILY, VIEW_PRODUCTS}:
            raise KeyError(name)
        return json_safe(list(db[name].find({}).sort("_id", 1)))
    finally:
        client.close()
