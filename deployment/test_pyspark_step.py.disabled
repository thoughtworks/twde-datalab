from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Test Comp") \
    .config("spark.driver.memory", "4g")  \
    .getOrCreate()


def table_attribute_formatter(table_name, column_names):
    return "{}".format(", ".join([table_name + '.{}'.format(x) for x in column_names]))


def loj_sql_formatter(right_attributes, on_column):
    return """SELECT left.*, {right_table_attributes} FROM left LEFT JOIN right ON left.{on_column} = right.{on_column}""".format(right_table_attributes=right_attributes, on_column=on_column)


def leftOuterJoin(left_table, right_table, on_column, columns_to_join):
    """ Takes two Spark DataFrames and performs a left outer join on them
        Returns a Spark DataFrame"""
    left_table.createOrReplaceTempView("left")
    right_table.createOrReplaceTempView("right")

    right_attributes = table_attribute_formatter("right", columns_to_join)

    sql_statement = loj_sql_formatter(right_attributes, on_column)

    # returns a spark dataframe
    train_items = spark.sql(sql_statement)
    return train_items


print("Running main function")
if __name__ == "__main__":
    tables = ["stores"]
    for t in tables:
        df = spark.read.csv("s3://twde-datalab/raw/{table}.csv".format(table=t))
        print(df.show())
        df = spark.write.csv("s3://twde-datalab/raw/{table}_test.csv".format(table=t))
