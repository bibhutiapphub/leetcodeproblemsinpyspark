from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()

    my_emp_data = [(101, "Aranza", "A", None)
        , (102, "Joren", "A", None)
        , (103, "Bibhuti", "A", 101)
        , (104, "Arun", "A", 101)
        , (105, "Abdul", "A", 101)
        , (106, "Anwesh", "A", 101)
        , (107, "Selva", "B", 101)
        , (108, "Peter", "B", 102)
        , (109, "Denise", "B", 102)
        , (110, "Johan", "B", 102)]

    my_emp_df = spark.createDataFrame(my_emp_data).toDF("Id", "Name", "Department", "ManagerId")

    employee_df = my_emp_df.alias("employee")
    managers_df = my_emp_df.alias("managers")

    join_cond = col("emp.ManagerId") == col("managers.Id")
    final_df = (my_emp_df.alias("emp")
                .join(my_emp_df.alias("managers"), join_cond, "inner")
                .groupBy("managers.Name")
                .agg(count("emp.Id").alias("reportees_counts"))
                .where("reportees_counts >= 5")
                .select("managers.Name")
                )
    final_df.show()
