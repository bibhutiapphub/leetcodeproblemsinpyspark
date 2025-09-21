from pyspark.sql import Window
from pyspark.sql.functions import *
from utils.spark_utils import *

spark = get_spark_session()

my_data = [(1, "Joe", 70000, 3)
    , (2, "Henry", 80000, 4)
    , (3, "Sam", 60000, None)
    , (4, "Max", 90000, None)]

data_df = spark.createDataFrame(my_data).toDF("Id", "Name", "Salary", "MangerId")
final_df = (data_df.alias("employee_df").join(data_df.alias("manager_df"), expr("employee_df.MangerId == manager_df.Id"))
            .where(expr("employee_df.Salary > manager_df.Salary"))
            .select(col("employee_df.Name").alias("Employee"))
            )
data_df.show()
final_df.show()
