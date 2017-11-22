from pyspark.sql import SparkSession
# import os
spark = SparkSession \
    .builder \
    .appName("Test Comp") \
    .config("spark.driver.memory", "4g")  \
    .getOrCreate()
# .config("fs.s3n.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
# .config("fs.s3n.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \


def table_attribute_formatter(table_name, column_names):
    return "{}".format(", ".join([table_name + '.{}'.format(x) for x in column_names]))


def loj_sql_formatter(right_attributes, on_column, extra_on_column=None):
    statement = """SELECT left.*, {right_table_attributes} FROM left LEFT JOIN right ON left.{on_column} = right.{on_column}""".format(right_table_attributes=right_attributes, on_column=on_column)
    if extra_on_column:
        statement += " AND left.{extra_on_column} = right.{extra_on_column}".format(extra_on_column=extra_on_column)
    return statement


def leftOuterJoin(left_table, right_table, on_column, columns_to_join, extra_on_column=None):
    """ Takes two Spark DataFrames and performs a left outer join on them
        Returns a Spark DataFrame"""
    left_table.createOrReplaceTempView("left")
    right_table.createOrReplaceTempView("right")

    right_attributes = table_attribute_formatter("right", columns_to_join)

    sql_statement = loj_sql_formatter(right_attributes, on_column, extra_on_column)

    # returns a spark dataframe
    train_items = spark.sql(sql_statement)
    return train_items


if __name__ == "__main__":
    # Load all tables from raw data
    tables = {'stores': None, 'items': None, 'train': None, 'transactions': None}
    for t in tables.keys():
        tables[t] = spark.read.csv("../data/{table}.csv".format(table=t), header='true')
    # Join train.csv and items.csv on item_nbr, preserving all columns
    bigTable = leftOuterJoin(tables['train'], tables['items'], 'item_nbr', ['family', 'class', 'perishable'])

    # Add stores.csv to big table on store_nbr, preserving all columns
    bigTable = leftOuterJoin(bigTable, tables['stores'], 'store_nbr', ['city', 'state', 'type', 'cluster'])

    # Add transactions to big table on store_nbr and date
    bigTable = leftOuterJoin(bigTable, tables['transactions'], 'store_nbr', ['transactions'], extra_on_column='date')

    # Add cities to big table on city and preserve all columns
    bigTable = leftOuterJoin(bigTable, tables['transactions'], 'store_nbr', ['transactions'], extra_on_column='date')

    # Write data to parquet file:
    bigTable.write.parquet('../data/bigTable')
