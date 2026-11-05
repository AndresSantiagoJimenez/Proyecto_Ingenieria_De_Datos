"""Unit tests for Gold Layer dimensional modeling and RFM analytics."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.data_platform.modeling.gold import GoldModeler
from src.data_platform.storage.session import get_spark_session


@pytest.fixture(scope="module")
def spark():
    """Module-level SparkSession for Gold unit tests."""
    session = get_spark_session(app_name="GoldUnitTest", master="local[2]")
    yield session
    session.stop()


def test_read_gold_raises_file_not_found():
    """Test read_gold raises FileNotFoundError when target path is missing."""
    mock_spark = MagicMock()
    modeler = GoldModeler(mock_spark)
    with pytest.raises(FileNotFoundError, match="Gold Delta table not found"):
        modeler.read_gold("non_existent", base_dir=Path("/non/existent/gold"))


def test_build_dim_customers(spark):
    """Test aggregation of customer dimension by customer_unique_id."""
    modeler = GoldModeler(spark)

    data = [
        ("C1", "UNIQ_A", "01151", "sao paulo", "SP"),
        ("C2", "UNIQ_A", "01151", "sao paulo", "SP"),  # Same unique customer
        ("C3", "UNIQ_B", "80000", "curitiba", "PR"),
    ]
    df = spark.createDataFrame(
        data,
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    )

    dim_df = modeler.build_dim_customers(df)
    assert dim_df.count() == 2

    uniq_a = dim_df.filter(dim_df.customer_unique_id == "UNIQ_A").first()
    assert uniq_a["total_registered_accounts"] == 2
    assert uniq_a["customer_state"] == "SP"
    assert "_gold_processed_at" in dim_df.columns


def test_build_fact_sales(spark):
    """Test fact_sales joins orders, items, and payments with total value calculations."""
    modeler = GoldModeler(spark)

    orders_data = [("ORD1", "CUST1", "delivered", "2018-01-01 10:00:00")]
    orders_df = spark.createDataFrame(
        orders_data,
        ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
    )

    items_data = [("ORD1", 1, "PROD1", "SELL1", 100.0, 15.0)]
    items_df = spark.createDataFrame(
        items_data,
        ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"],
    )

    payments_data = [("ORD1", "credit_card", 3, 115.0)]
    payments_df = spark.createDataFrame(
        payments_data,
        ["order_id", "payment_type", "payment_installments", "payment_value"],
    )

    fact_df = modeler.build_fact_sales(orders_df, items_df, payments_df)
    assert fact_df.count() == 1

    row = fact_df.first()
    assert row["order_id"] == "ORD1"
    assert row["total_item_value"] == 115.0
    assert row["primary_payment_type"] == "credit_card"
    assert row["order_total_payment"] == 115.0


def test_build_dim_logistics(spark):
    """Test logistics delivery duration and delay flag calculation."""
    modeler = GoldModeler(spark)

    orders_data = [
        (
            "ORD1",
            "CUST1",
            "delivered",
            "2018-01-01 10:00:00",
            "2018-01-10 12:00:00",  # Delivered on Jan 10
            "2018-01-08 00:00:00",  # Estimated on Jan 8 -> 2 days delay
        ),
        (
            "ORD2",
            "CUST2",
            "delivered",
            "2018-01-01 10:00:00",
            "2018-01-05 12:00:00",  # Delivered on Jan 5
            "2018-01-10 00:00:00",  # Estimated on Jan 10 -> On time
        ),
    ]
    orders_df = spark.createDataFrame(
        orders_data,
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )

    logistics_df = modeler.build_dim_logistics(orders_df)
    assert logistics_df.count() == 2

    delayed_order = logistics_df.filter(logistics_df.order_id == "ORD1").first()
    assert delayed_order["is_delayed"] == 1
    assert delayed_order["delivery_delay_days"] == 2

    on_time_order = logistics_df.filter(logistics_df.order_id == "ORD2").first()
    assert on_time_order["is_delayed"] == 0
    assert on_time_order["delivery_delay_days"] == -5


def test_build_dim_customer_rfm(spark):
    """Test customer RFM segmentation based on recency, frequency, and monetary."""
    modeler = GoldModeler(spark)

    cust_data = [("C1", "UNIQ1", "sao paulo", "SP")]
    cust_df = spark.createDataFrame(
        cust_data,
        ["customer_id", "customer_unique_id", "customer_city", "customer_state"],
    )

    orders_data = [
        ("O1", "C1", "2018-05-01 10:00:00"),
        ("O2", "C1", "2018-06-01 10:00:00"),
    ]
    orders_df = spark.createDataFrame(orders_data, ["order_id", "customer_id", "order_purchase_timestamp"])

    pay_data = [("O1", 100.0), ("O2", 150.0)]
    pay_df = spark.createDataFrame(pay_data, ["order_id", "payment_value"])

    rfm_df = modeler.build_dim_customer_rfm(
        cust_df,
        orders_df,
        pay_df,
        reference_date="2018-06-15",
    )
    assert rfm_df.count() == 1

    row = rfm_df.first()
    assert row["frequency"] == 2
    assert row["monetary_value"] == 250.0
    assert row["recency_days"] == 14
    assert row["rfm_segment"] == "Champions"
