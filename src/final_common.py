import json
from datetime import datetime, date
from bson import ObjectId


def json_safe(value):
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    return value


def money_expr(field):
    return {
        "$convert": {
            "input": {"$replaceAll": {"input": {"$toString": {"$ifNull": [field, "0"]}}, "find": ",", "replacement": ""}},
            "to": "double",
            "onError": 0.0,
            "onNull": 0.0,
        }
    }


def parse_items(items):
    if isinstance(items, list):
        return items
    if isinstance(items, dict):
        return [items]
    if not items:
        return []
    try:
        parsed = json.loads(items)
        return parsed if isinstance(parsed, list) else [parsed]
    except Exception:
        return []
