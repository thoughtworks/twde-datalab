import sys
import os
import pytest
import logging
# import numpy as np
# from pytest import approx
from pyspark.sql import SparkSession
sys.path.append(os.path.join('..', 'src'))
sys.path.append(os.path.join('src'))
import merger


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark context."""

    spark = (SparkSession
             .builder
             .config("spark.driver.memory", "4g")
             .appName('pytest-pyspark-local-testing')
             .getOrCreate())
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark


def test_table_attribute_formatter():
    table_name = 'table'
    column_names = ['attrib1', 'attrib2']
    formatted_string = merger.table_attribute_formatter(table_name, column_names)
    assert formatted_string == 'table.attrib1, table.attrib2'


def test_loj_sql_formatter():
    right_attributes = "a.x, a.y, a.z"
    on_column = "qrs"
    sql_statement = merger.loj_sql_formatter(right_attributes, on_column)
    assert sql_statement == "SELECT left.*, a.x, a.y, a.z FROM left LEFT JOIN right ON left.qrs = right.qrs"


def test_join_datasets_on_column(spark_session):
    train_headers = ['id', 'date', 'store_nbr', 'item_nbr', 'unit_sales', 'onpromotion']
    train_data1 = ['0', '2013-01-01', '25', '1111', '7.0', '0']
    train_data2 = ['1', '2013-01-01', '25', '9999', '1.0', '0']
    mockTrain = spark_session.createDataFrame([train_data1, train_data2], schema=train_headers)

    items_headers = ['item_nbr', 'family', 'class', 'perishable']
    items_data1 = ['1111', 'A', 'C', '1']
    items_data2 = ['1234', 'B', 'C', '0']
    mockItems = spark_session.createDataFrame([items_data1, items_data2], schema=items_headers)
    mockTrain.createOrReplaceTempView("train")
    mockItems.createOrReplaceTempView("items")
    on_column = "item_nbr"
    columns = ["family", "class", "perishable"]
    train_items_merged = merger.leftOuterJoin(mockTrain, mockItems, on_column, columns)
    assert train_items_merged.count() == 2
    assert train_items_merged.columns == ['id', 'date', 'store_nbr', 'item_nbr', 'unit_sales', 'onpromotion', 'family', 'class', 'perishable']
    assert train_items_merged.count() == 2
    # asserting that the family column of item 9999 is null, because we have
    # no store, family, or class data for it
    assert train_items_merged.filter(train_items_merged.item_nbr == 9999).collect()[0].family is None
    assert train_items_merged.filter(train_items_merged.item_nbr == 9999).collect()[0].id is not None
    assert train_items_merged.filter(train_items_merged.item_nbr == 1111).collect()[0].family is not None
