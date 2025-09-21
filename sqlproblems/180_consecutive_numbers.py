from pyspark.sql import Window
from pyspark.sql.functions import *
from utils.spark_utils import *

spark = get_spark_session()

my_data = [(1, 3)
    , (2, 3)
    , (3, 3)
    , (4, 2)
    , (5, 2)
    , (6, 2)
    , (7, 2)
    , (8, 1)]

my_df = spark.createDataFrame(my_data).toDF("id","num")
lagColumn = lag(col("num")).over(Window.orderBy(col("id")))
leadColumn = lead(col("num")).over(Window.orderBy(col("id")))
final_df = (my_df.withColumn("lead_value",leadColumn)
            .withColumn("lag_value",lagColumn)
            .where((col("num") == col("lead_value")) & (col("num") == col("lag_value")))
            .select(col("num").alias("ConsecutiveNums")).distinct()
            )

final_df.show()

# Other Approach using JOIN
my_final_df = (my_df.alias("my_df1").join(my_df.alias("my_df2"),expr("my_df1.id == my_df2.id+1"))
               .join(my_df.alias("my_df3"),expr("my_df1.id == my_df3.id+2"))
               .where(expr("my_df1.num == my_df3.num"))
               .select(col("my_df1.num").alias("ConsecutiveNums")).distinct())
my_final_df.show()