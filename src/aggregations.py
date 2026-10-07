from pymongo import MongoClient
from config.settings import MONGO_URI, DB_NAME, COLLECTION_VALIDATED
from src.final_common import json_safe, money_expr

AGGREGATION_DEFINITIONS = {
    "sales_by_city": "إجمالي المبيعات وعدد الطلبات حسب المدينة",
    "top_products": "أفضل المنتجات حسب الكمية والقيمة",
    "top_customers": "أفضل العملاء حسب إجمالي الإنفاق",
    "sales_by_period": "المبيعات حسب الشهر",
    "orders_by_status": "توزيع الطلبات حسب الحالة",
}


def _client():
    c = MongoClient(MONGO_URI)
    return c, c[DB_NAME][COLLECTION_VALIDATED]


def run_aggregation(name):
    client, col = _client()
    try:
        if name == "sales_by_city":
            pipeline = [{"$group": {"_id": "$city", "orders": {"$sum": 1}, "sales": {"$sum": money_expr("$total_amount")}}}, {"$sort": {"sales": -1}}]
        elif name == "top_customers":
            pipeline = [{"$group": {"_id": "$customer_id", "orders": {"$sum": 1}, "sales": {"$sum": money_expr("$total_amount")}}}, {"$sort": {"sales": -1}}, {"$limit": 20}]
        elif name == "sales_by_period":
            pipeline = [{"$set": {"_date": {"$dateFromString": {"dateString": "$order_date", "onError": None, "onNull": None}}}}, {"$match": {"_date": {"$ne": None}}}, {"$group": {"_id": {"$dateToString": {"format": "%Y-%m", "date": "$_date"}}, "orders": {"$sum": 1}, "sales": {"$sum": money_expr("$total_amount")}}}, {"$sort": {"_id": 1}}]
        elif name == "orders_by_status":
            pipeline = [{"$group": {"_id": "$status", "orders": {"$sum": 1}, "sales": {"$sum": money_expr("$total_amount")}}}, {"$sort": {"orders": -1}}]
        elif name == "top_products":
            pipeline = [{"$unwind": {"path": "$items_parsed", "preserveNullAndEmptyArrays": False}}, {"$group": {"_id": "$items_parsed.item_id", "quantity": {"$sum": {"$convert": {"input": "$items_parsed.quantity", "to": "double", "onError": 1, "onNull": 1}}}, "sales": {"$sum": {"$multiply": [{"$convert": {"input": "$items_parsed.quantity", "to": "double", "onError": 1, "onNull": 1}}, {"$convert": {"input": "$items_parsed.price", "to": "double", "onError": 0, "onNull": 0}}]}}}}, {"$sort": {"sales": -1}}, {"$limit": 20}]
        else:
            raise KeyError(f"Unknown aggregation: {name}")
        result = list(col.aggregate(pipeline, allowDiskUse=True))
        return {"name": name, "description": AGGREGATION_DEFINITIONS[name], "count": len(result), "results": json_safe(result)}
    finally:
        client.close()
