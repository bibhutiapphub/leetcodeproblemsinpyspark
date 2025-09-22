from pyspark.sql import Window
from pyspark.sql.functions import *
from utils.spark_utils import *

spark = get_spark_session()

emp_data = [(1, "Joe", 70000, 1)
    , (2, "Jim", 90000, 1)
    , (3, "Henry", 80000, 2)
    , (4, "Sam", 60000, 2)
    , (5, "Max", 90000, 1)]

dept_data = [(1, "IT"), (2, "Sales")]

emp_df = spark.createDataFrame(emp_data).toDF("Id","Name","Salary","departmentId")
dept_df = spark.createDataFrame(dept_data).toDF("Id","Name")
join_cond = emp_df.departmentId == dept_df.Id

partition_window = Window.partitionBy(dept_df.Name).orderBy(desc(emp_df.Salary))

final_df = (emp_df.join(dept_df,join_cond)
             .withColumn("rank_salary", dense_rank().over(partition_window))
             .where("rank_salary == 1")
             .select(dept_df.Name.alias("Department")
                     ,emp_df.Name.alias("Employee")
                     ,emp_df.Salary))
final_df.show()

# Alternative approach
department_max_sal_rows = emp_df.groupBy("departmentId").agg(max("Salary").alias("maxSalary")).collect()

max_salary_list = [int(row.maxSalary) for row in department_max_sal_rows]
departments_list = [int(row.departmentId) for row in department_max_sal_rows]

another_final_df = (emp_df.join(dept_df,join_cond)
                    .where(emp_df.departmentId.isin(departments_list) & emp_df.Salary.isin(max_salary_list))
                    .select(dept_df.Name.alias("Department")
                            , emp_df.Name.alias("Employee")
                            , emp_df.Salary))

another_final_df.show()