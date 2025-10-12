from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    salary_data = [(1, 1, 9000, "2017-03-31")
        , (2, 2, 6000, "2017-03-31")
        , (3, 3, 10000, "2017-03-31")
        , (4, 1, 7000, "2017-02-28")
        , (5, 2, 6000, "2017-02-28")
        , (6, 3, 8000, "2017-02-28")
                   ]

    employee_data = [(1, 1)
        , (2, 2)
        , (3, 2)]

    salary_df = (spark.createDataFrame(salary_data)
                 .toDF("id", "employee_id", "amount", "pay_date")
                 .withColumn("pay_date", to_date("pay_date", "yyyy-M-d"))
                 .withColumn("pay_date_month_year", concat(date_format(col("pay_date"),"MM"), lit("-"), date_format(col("pay_date"),"yyyy")))
                 .select("id","employee_id","amount","pay_date_month_year")
                 )

    employee_df = (spark.createDataFrame(employee_data)
                   .toDF("employee_id", "department_id"))

    emp_dept_join_cond = salary_df["employee_id"] == employee_df["employee_id"]
    monthly_company_average_partition = avg("amount").over(Window.partitionBy("pay_date_month_year"))
    monthly_company_dept_average_partition = avg("amount").over(Window.partitionBy(["pay_date_month_year", "department_id"]))

    joined_df = (salary_df
                 .join(employee_df,emp_dept_join_cond,"inner")
                 .withColumn("comp_average", monthly_company_average_partition)
                 .withColumn("dept_comp_average", monthly_company_dept_average_partition)
                 .withColumn("comparison", when(col("dept_comp_average") > col("comp_average"), lit("higher"))
                             .when(col("dept_comp_average") < col("comp_average"), lit("lower"))
                             .otherwise(lit("same")))
                 .select("pay_date_month_year","department_id","comparison")
                 .distinct()
                 )
    joined_df.show()
