from pyspark.sql import Window
from pyspark.sql.functions import *
from utils.spark_utils import *

spark = get_spark_session()

emp_data = [(1, "Joe@gmail.com")
    , (2, "Joe@gmail.com")
    , (3, "Henry@gmail.com")]

emp_df = spark.createDataFrame(emp_data).toDF("Id","Email")

# Using distinct
final_df_distinct = emp_df.dropDuplicates(["Email"])
final_df_distinct.show()

# Using Window functions
partition_window = Window.partitionBy("Email").orderBy("Email")
final_df = (emp_df.withColumn("row_number",row_number().over(partition_window))
            .where(col("row_number") == 1)
            .select("Id","Email")
            .orderBy("Id"))

final_df.show()