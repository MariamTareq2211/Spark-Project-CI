import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark_job import clean_data

SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True),
])


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("pyspark-ci-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_valid_records_are_kept(spark):
    df = spark.createDataFrame([("Ali", 100.0), ("Sara", 50.0), ("Mohamed", 60.0)], SCHEMA)
    result = clean_data(df)
    assert result.count() == 3


def test_amount_less_or_equal_zero_removed(spark):
    df = spark.createDataFrame([("Ali", 0.0), ("Sara", -5.0), ("Omar", 10.0)], SCHEMA)
    result = clean_data(df).collect()
    assert len(result) == 1
    assert result[0]["name"] == "Omar"


def test_null_names_removed(spark):
    df = spark.createDataFrame([(None, 100.0), ("Sara", 50.0)], SCHEMA)
    result = clean_data(df).collect()
    assert len(result) == 1
    assert result[0]["name"] == "Sara"


def test_amount_with_tax_calculated_correctly(spark):
    df = spark.createDataFrame([("Ali", 100.0)], SCHEMA)
    row = clean_data(df).collect()[0]
    assert row["amount_with_tax"] == pytest.approx(120.0)

def test_null_amount_removed(spark):
    df = spark.createDataFrame([("Ali", None), ("Sara", 50.0)], SCHEMA)
    result = clean_data(df).collect()
    assert len(result) == 1
    assert result[0]["name"] == "Sara"

def test_output_columns(spark):
    df = spark.createDataFrame([("Ali", 100.0)], SCHEMA)
    assert clean_data(df).columns == ["name", "amount", "amount_with_tax"]