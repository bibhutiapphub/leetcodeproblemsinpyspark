from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    employee_data = [(1, "A", 2341)
        , (2, "A", 341)
        , (3, "A", 15)
        , (4, "A", 15314)
        , (5, "A", 451)
        , (6, "A", 513)
        , (7, "B", 15)
        , (8, "B", 13)
        , (9, "B", 1154)
        , (10, "B", 1345)
        , (11, "B", 1221)
        , (12, "B", 234)
        , (13, "C", 2345)
        , (14, "C", 2645)
        , (15, "C", 2645)
        , (16, "C", 2652)
        , (17, "C", 65)
        , (18, "D", 90)
        , (19, "D", 350)
        , (20, "D", 400)
        , (21, "D", 100)
        , (22, "E", 20)
        , (23, "E", 95)
        , (24, "E", 45)
        , (25, "F", 70)
        , (26, "F", 80)
        , (27, "G", 150)
                     ]

    employee_df = spark.createDataFrame(employee_data).toDF("Id", "Company", "Salary")

    rownum_window = row_number().over(Window.partitionBy("Company").orderBy("Salary"))
    count_window = count("Company").over(Window.partitionBy("Company"))
    final_df = (employee_df.withColumn("salary_rownum", rownum_window)
                .withColumn("company_count", count_window)
                .withColumn("median_positions", when(col("company_count") % 2 == 0,
                                                     array((col("company_count") / 2).cast("integer"),
                                                           ((col("company_count") / 2) + 1).cast("integer")))
                            .otherwise(array(ceiling(col("company_count") / 2)))
                            )
                .where(array_contains(col("median_positions"), col("salary_rownum")))
                .select("Id", "Company", "Salary")
                )
    final_df.show(50)
