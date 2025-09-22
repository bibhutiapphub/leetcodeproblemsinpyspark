from pyspark.sql import Window
from pyspark.sql.functions import *
from utils.spark_utils import *

spark = get_spark_session()

my_data = [(1, "a@gmail.com"),(2, "b@gmail.com"),(3, "a@gmail.com")]
my_df = spark.createDataFrame(my_data).toDF("Id","Email")
final_df = (my_df.groupby("Email")
            .agg(count("*").alias("email_count"))
            .where("email_count > 1")
            .select("Email"))
final_df.show()

# Using Window functions
partition_window = Window.partitionBy("Email").orderBy(desc("Email"))
my_final_df = (my_df.withColumn("row_number",row_number().over(partition_window))
               .where("row_number > 1")
               .select("Email"))
my_final_df.show()