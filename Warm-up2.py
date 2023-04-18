from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# http://localhost:4040"

# This had failed to work because I wasn't launching it in the venv. Had to type it into the terminal

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("Exercise1")\
    .config("spark.sql.autoBroadcastJoinThreshold", -1)\
    .config("spark.executor.memory", "500mb")\
    .getOrCreate()

# Reminds me of CSS Selectors.
products_table = spark.read.parquet("./DatasetToCompleteTheSixSparkExercises/products_parquet")
sales_table = spark.read.parquet("./DatasetToCompleteTheSixSparkExercises/sales_parquet")
sellers_table = spark.read.parquet("./DatasetToCompleteTheSixSparkExercises/sellers_parquet")

print(f"Number of Orders: {sales_table.count()}")
print(f"Number of Sellers: {sellers_table.count()}")
print(f"Number of Products: {products_table.count()}")

