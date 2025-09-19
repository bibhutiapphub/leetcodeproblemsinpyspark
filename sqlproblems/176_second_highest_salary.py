from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

spark = get_spark_session()

employee_data = [(1, 100), (2, 200), (3, 300), (4, 400)]
employee_data_2 = [(1, 100)]
employee_df = spark.createDataFrame(employee_data_2).toDF("id", "salary")
row_number_window = Window.orderBy(desc(col("salary")))
# Using window function
final_df = (employee_df
            .withColumn("row_num", row_number().over(row_number_window))
            .where("row_num = 2")
            .select("salary"))
final_df.show()

max_salary = employee_df.select(max("salary").alias("highestSalary")).first()[0]
# Using max function
final_df_2 = (employee_df
              .where(col("salary") != max_salary)
              .select(max("salary").alias("secondHighestSalary")))
final_df_2.show()
