def test_final_modules_importable():
    from src.final_queries import QUERY_DEFINITIONS
    from src.aggregations import AGGREGATION_DEFINITIONS
    from src.materialized_views import VIEW_DAILY, VIEW_PRODUCTS
    assert len(QUERY_DEFINITIONS) >= 5
    assert len(AGGREGATION_DEFINITIONS) >= 5
    assert VIEW_DAILY == "daily_sales_summary"
    assert VIEW_PRODUCTS == "top_products_summary"
