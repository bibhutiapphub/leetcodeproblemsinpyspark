from pyspark.sql import Window
from pyspark.sql.functions import *
from utils.spark_utils import *

spark = get_spark_session()

my_data = [(1, 3.50)
    , (2, 3.65)
    , (3, 4.00)
    , (4, 3.85)
    , (5, 4.00)
    , (6, 3.65)]

my_df = spark.createDataFrame(my_data).toDF("id","score")
partition_window = Window.orderBy(desc("score"))
final_df = my_df.withColumn("rank",dense_rank().over(partition_window))
final_df.show()
