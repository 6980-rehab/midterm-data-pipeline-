from datetime import datetime, timedelta
from pymongo import ASCENDING, DESCENDING
from config.settings import MONGO_URI, DB_NAME, COLLECTION_VALIDATED
from pymongo import MongoClient
from src.final_common import json_safe, money_expr


# ============================================================
# Query Definitions
# ============================================================

QUERY_DEFINITIONS = {
    "orders_by_status": "توزيع الطلبات حسب الحالة",
    "orders_by_city": "عدد الطلبات حسب المدينة",
    "customer_orders": "طلبات عميل محدد",
    "orders_in_period": "الطلبات خلال فترة زمنية",
    "high_value_orders": "الطلبات ذات القيمة المرتفعة",
}


# ============================================================
# MongoDB Connection
# ============================================================

def _collection():
    client = MongoClient(MONGO_URI)
    return client, client[DB_NAME][COLLECTION_VALIDATED]


# ============================================================
# Run Queries
# ============================================================

def run_query(name, params=None):
    params = params or {}

    client, col = _collection()

    try:

        # ----------------------------------------------------
        # 1. Orders by Status
        # ----------------------------------------------------
        if name == "orders_by_status":

            docs = list(
                col.aggregate([
                    {
                        "$group": {
                            "_id": "$status",
                            "count": {"$sum": 1},
                            "sales": {
                                "$sum": money_expr("$total_amount")
                            }
                        }
                    },
                    {
                        "$sort": {
                            "count": -1
                        }
                    }
                ])
            )

        # ----------------------------------------------------
        # 2. Orders by City
        # ----------------------------------------------------
        elif name == "orders_by_city":

            docs = list(
                col.aggregate([
                    {
                        "$group": {
                            "_id": "$city",
                            "count": {"$sum": 1},
                            "sales": {
                                "$sum": money_expr("$total_amount")
                            }
                        }
                    },
                    {
                        "$sort": {
                            "sales": -1
                        }
                    }
                ])
            )

        # ----------------------------------------------------
        # 3. Orders of Specific Customer
        # ----------------------------------------------------
        elif name == "customer_orders":

            customer_id = str(
                params.get("customer_id", "")
            ).strip()

            if not customer_id:
                raise ValueError(
                    "customer_id is required"
                )

            docs = list(
                col.find(
                    {
                        "customer_id": customer_id
                    }
                )
                .sort(
                    "order_date",
                    DESCENDING
                )
                .limit(
                    int(
                        params.get(
                            "limit",
                            100
                        )
                    )
                )
            )

        # ----------------------------------------------------
        # 4. Orders in Date Period
        # ----------------------------------------------------
        elif name == "orders_in_period":

            start = params.get("start")
            end = params.get("end")

            if not start or not end:
                raise ValueError(
                    "start and end are required as YYYY-MM-DD"
             )
            docs = list(
                col.find(
                    {
                        "order_date": {
                            "$gte": start,
                            "$lte": end
                        }
                    }
                )
                .sort(
                    "order_date",
                    ASCENDING
                )
                .limit(
                    int(
                        params.get(
                            "limit",
                            1000
                        )
                    )
                )
            )

        # ----------------------------------------------------
        # 5. High Value Orders
        # ----------------------------------------------------
        elif name == "high_value_orders":

            minimum = float(
                params.get(
                    "minimum",
                    100000
                )
            )

            docs = list(
                col.find(
                    {
                        "$expr": {
                            "$gte": [
                                money_expr(
                                    "$total_amount"
                                ),
                                minimum
                            ]
                        }
                    }
                )
                .sort(
                    "total_amount",
                    DESCENDING
                )
                .limit(
                    int(
                        params.get(
                            "limit",
                            100
                        )
                    )
                )
            )

        else:
            raise KeyError(
                f"Unknown query: {name}"
            )

        return {
            "name": name,
            "description": QUERY_DEFINITIONS[name],
            "count": len(docs),
            "results": json_safe(docs)
        }

    finally:
        client.close()


# ============================================================
# Create Final Indexes
# ============================================================

def create_indexes():

    client, col = _collection()

    try:

        created = []

        specs = [

            # Index 1
            (
                [
                    ("customer_id", ASCENDING)
                ],
                {
                    "name": "idx_customer_id"
                }
            ),

            # Index 2
            (
                [
                    ("order_date", DESCENDING)
                ],
                {
                    "name": "idx_order_date"
                }
            ),

            # Index 3 - Compound Index
            (
                [
                    ("city", ASCENDING),
                    ("status", ASCENDING)
                ],
                {
                    "name": "idx_city_status_compound"
                }
            ),
        ]

        for keys, opts in specs:

            created.append(
                col.create_index(
                    keys,
                    **opts
                )
            )

        return {
            "created_indexes": created,
            "indexes": list(
                col.index_information().keys()
            )
        }

    finally:
        client.close()


# ============================================================
# Explain Query
# ============================================================

def explain_query(
    name,
    params=None,
    hint=None
):

    params = params or {}

    client, col = _collection()

    try:

        # ----------------------------------------------------
        # Customer Orders
        # ----------------------------------------------------
        if name == "customer_orders":

            customer_id = str(
                params.get(
                    "customer_id",
                    ""
                )
            ).strip()
            if not customer_id:
                raise ValueError(
                    "customer_id is required"
                )

            find_command = {
                "find": col.name,
                "filter": {
                    "customer_id": customer_id
                },
                "limit": int(
                    params.get(
                        "limit",
                        100
                    )
                )
            }

            if hint:
                find_command["hint"] = hint

            return json_safe(
                col.database.command(
                    "explain",
                    find_command,
                    verbosity="executionStats"
                )
            )

        # ----------------------------------------------------
        # Orders in Period
        # ----------------------------------------------------
        elif name == "orders_in_period":

            start = params.get("start")
            end = params.get("end")

            if not start or not end:
                raise ValueError(
                    "start and end are required"
                )

            find_command = {
                "find": col.name,
                "filter": {
                    "order_date": {
                        "$gte": start,
                        "$lte": end
                    }
                },
                "limit": int(
                    params.get(
                        "limit",
                        1000
                    )
                )
            }

            if hint:
                find_command["hint"] = hint

            return json_safe(
                col.database.command(
                    "explain",
                    find_command,
                    verbosity="executionStats"
                )
            )

        # ----------------------------------------------------
        # Orders by City
        # ----------------------------------------------------
        elif name == "orders_by_city":

            city = params.get(
                "city",
                ""
            )

            pipeline = [
                {
                    "$match": {
                        "city": city
                    }
                },
                {
                    "$group": {
                        "_id": "$status",
                        "count": {
                            "$sum": 1
                        }
                    }
                }
            ]

            aggregate_kwargs = {
                "explain": True,
                "cursor": {}
            }

            # Apply Compound Index Hint
            if hint:
                aggregate_kwargs["hint"] = hint

            command = col.database.command(
                "aggregate",
                col.name,
                pipeline=pipeline,
                **aggregate_kwargs
            )

            return json_safe(
                command
            )

        else:

            raise ValueError(
                "Explain is supported for "
                "customer_orders, "
                "orders_in_period, "
                "and orders_by_city"
            )

    finally:
        client.close()


# ============================================================
# Drop Final Indexes
# ============================================================

def drop_final_indexes():

    client, col = _collection()

    try:

        indexes_to_drop = [
            "idx_customer_id",
            "idx_order_date",
            "idx_city_status_compound"
        ]

        for name in indexes_to_drop:

            try:
                col.drop_index(name)

            except Exception:
                # Index may not exist
                pass

        return {
            "remaining_indexes": list(
                col.index_information().keys()
            )
        }

    finally:
        client.close()
# ============================================================
# Explain Before / After
# ============================================================

def explain_before_after(
    name,
    params=None
):

    # --------------------------------------------------------
    # 1. Remove final indexes
    # --------------------------------------------------------

    drop_final_indexes()

    # --------------------------------------------------------
    # 2. Explain BEFORE indexes
    # --------------------------------------------------------

    before = explain_query(
        name,
        params
    )

    # --------------------------------------------------------
    # 3. Select correct index
    # --------------------------------------------------------

    hints = {

        "customer_orders":
            "idx_customer_id",

        "orders_in_period":
            "idx_order_date",

        "orders_by_city":
            "idx_city_status_compound"
    }

    index_name = hints.get(name)

    if not index_name:
        raise ValueError(
            f"No index configured for query: {name}"
        )

    # --------------------------------------------------------
    # 4. Create indexes
    # --------------------------------------------------------

    create_indexes()

    # --------------------------------------------------------
    # 5. Explain AFTER indexes
    # --------------------------------------------------------

    after = explain_query(
        name,
        params,
        hint=index_name
    )

    # --------------------------------------------------------
    # 6. Return comparison
    # --------------------------------------------------------

    return {
        "query": name,
        "before_indexes": before,
        "after_indexes": after,
        "index_used": index_name
    }