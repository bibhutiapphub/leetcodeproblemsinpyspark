from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    emp_salary_data = ((1, 1, 20)
                           , (2, 1, 20)
                           , (1, 2, 30)
                           , (2, 2, 30)
                           , (3, 2, 40)
                           , (1, 3, 40)
                           , (3, 3, 60)
                           , (1, 4, 60)
                           , (1, 5, 80)
                           , (3, 4, 70))

    empl_partition = Window.partitionBy("Id")
    max_month_emp_partition = max("Month").over(empl_partition)
    cum_salary_emp_partition = sum("Salary").over(empl_partition.orderBy("Month"))
    emp_row_numbers = row_number().over(empl_partition.orderBy("Month"))
    emp_salary_df = (spark.createDataFrame(emp_salary_data)
                     .toDF("Id", "Month", "Salary")
                     .orderBy("Id"
                              , "Month")
                     .withColumn("Max_Month", max_month_emp_partition)
                     .withColumn("row_num", emp_row_numbers)
                     .where((col("Month") < col("Max_Month")) & (col("row_num") <= 3))
                     .withColumn("Cum_Salary", cum_salary_emp_partition)
                     .select(col("Id")
                             , col("Month")
                             , col("Cum_Salary").alias("Salary"))
                     .orderBy(asc("Id"), desc("Month"))
                     )
    emp_salary_df.show()
