"""Integration tests for Gold Layer Delta tables and Spark SQL queries."""

from pathlib import Path

import pytest

from src.data_platform.config import settings
from src.data_platform.modeling.gold import GoldModeler
from src.data_platform.storage.session import get_spark_session


@pytest.fixture(scope="module")
def spark():
    """Module-level SparkSession for Gold Delta integration testing."""
    session = get_spark_session(app_name="GoldDeltaIntegrationTest", master="local[2]")
    yield session
    session.stop()


def test_gold_star_schema_and_sql_queries_end_to_end(spark, tmp_path: Path):
    """Test full cycle: read sample files, build Gold star schema, persist to Delta, query via SQL."""
    gold_dir = tmp_path / "gold"
    modeler = GoldModeler(spark)

    sample_dir = settings.sample_path
    customers_file = sample_dir / "olist_customers_sample.csv"
    orders_file = sample_dir / "olist_orders_sample.csv"
    items_file = sample_dir / "olist_order_items_sample.csv"
    payments_file = sample_dir / "olist_order_payments_sample.csv"

    assert customers_file.exists()
    assert orders_file.exists()
    assert items_file.exists()
    assert payments_file.exists()

    cust_df = spark.read.format("csv").option("header", "true").load(str(customers_file))
    orders_df = spark.read.format("csv").option("header", "true").load(str(orders_file))
    items_df = spark.read.format("csv").option("header", "true").load(str(items_file))
    payments_df = spark.read.format("csv").option("header", "true").load(str(payments_file))

    # 1. Build and persist dim_customers
    dim_cust_df = modeler.build_dim_customers(cust_df)
    res_cust = modeler.save_gold_table(dim_cust_df, "dim_customers", base_dir=gold_dir)
    assert res_cust.status == "SUCCESS"
    assert res_cust.records_count > 0

    # 2. Build and persist fact_sales
    fact_sales_df = modeler.build_fact_sales(orders_df, items_df, payments_df)
    res_sales = modeler.save_gold_table(fact_sales_df, "fact_sales", base_dir=gold_dir)
    assert res_sales.status == "SUCCESS"
    assert res_sales.records_count == 5

    # 3. Build and persist dim_customer_rfm
    rfm_df = modeler.build_dim_customer_rfm(cust_df, orders_df, payments_df)
    res_rfm = modeler.save_gold_table(rfm_df, "dim_customer_rfm", base_dir=gold_dir)
    assert res_rfm.status == "SUCCESS"
    assert res_rfm.records_count > 0

    # 4. Build and persist dim_logistics
    logistics_df = modeler.build_dim_logistics(orders_df, cust_df)
    res_logistics = modeler.save_gold_table(logistics_df, "dim_logistics", base_dir=gold_dir)
    assert res_logistics.status == "SUCCESS"
    assert res_logistics.records_count == 5

    # 5. Read back from Delta Lake
    loaded_sales = modeler.read_gold("fact_sales", base_dir=gold_dir)
    assert loaded_sales.count() == 5
    assert "total_item_value" in loaded_sales.columns

    # 6. Execute analytical Spark SQL query joining Fact and Dim
    sql_query = """
        SELECT c.customer_state,
               COUNT(DISTINCT s.order_id) as total_orders,
               ROUND(SUM(s.total_item_value), 2) as revenue
        FROM fact_sales s
        JOIN dim_customers c ON s.customer_id = c.sample_customer_id
        GROUP BY c.customer_state
        ORDER BY revenue DESC
    """
    analytics_df = modeler.query_gold(sql_query)
    assert analytics_df.count() > 0
    first_row = analytics_df.first()
    assert first_row["revenue"] > 0
