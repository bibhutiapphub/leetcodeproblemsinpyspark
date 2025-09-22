from pyspark.sql import Window
from pyspark.sql.functions import *
from utils.spark_utils import *

spark = get_spark_session()

emp_data = [(1, "Joe", 85000, 1)
    , (2, "Janet", 69000, 1)
    , (3, "Henry", 80000, 2)
    , (4, "Sam", 60000, 2)
    , (5, "Max", 90000, 1)
    , (6, "Randy", 85000, 1)
    , (7, "Will", 70000, 1)]


dept_data = [(1, "IT"), (2, "Sales")]
emp_df = spark.createDataFrame(emp_data).toDF("Id","Name","Salary","departmentId")
dept_df = spark.createDataFrame(dept_data).toDF("Id","Name")
join_cond = emp_df.departmentId == dept_df.Id

partition_window = Window.partitionBy(dept_df.Name).orderBy(asc(dept_df.Id),desc(emp_df.Salary))

final_df = (emp_df.join(dept_df,join_cond)
             .withColumn("rank_salary", dense_rank().over(partition_window))
             .where(col("rank_salary")<=3)
             .select(dept_df.Name.alias("Department")
                     ,emp_df.Name.alias("Employee")
                     ,emp_df.Salary))

final_df.show()