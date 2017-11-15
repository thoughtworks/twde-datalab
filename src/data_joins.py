from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("Test Comp") \
    .config("spark.driver.memory", "4g")  \
    .getOrCreate()

train = spark.read.csv("../data/train.csv", inferSchema=True, header=True).cache()
items = spark.read.csv("../data/items.csv", inferSchema=True, header=True).cache()

train.createOrReplaceTempView("train")
items.createOrReplaceTempView("items")

# returns a spark dataframe
train_items = spark.sql("""SELECT
                    train.*,
                    items.family, items.class, items.perishable
                    FROM train
                    LEFT JOIN items
                    ON train.item_nbr = items.item_nbr
                """)

stores = spark.read.csv("../data/stores.csv", inferSchema=True, header=True).cache()
transactions = spark.read.csv("../data/transactions.csv", inferSchema=True, header=True).cache()
stores.createOrReplaceTempView("stores")
transactions.createOrReplaceTempView("transactions")
# returns a spark dataframe
stores_transactions = spark.sql("""SELECT
                    stores.*,
                    transactions.date, transactions.transactions
                    FROM stores
                    LEFT JOIN transactions
                    ON stores.store_nbr = transactions.store_nbr
                """)
