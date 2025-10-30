from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    name_continent_data = [("Jack", "America")
        , ("Pascal", "Europe")
        , ("Xi", "Asia")
        , ("Jane", "America")]

    name_continent_df = (spark.createDataFrame(name_continent_data)
                         .toDF("name", "continent")
                         )
    continent_columns = [continent_row.continent for continent_row in
                         name_continent_df.select("continent").distinct().orderBy("continent").collect()]

    grouped_values_df = (name_continent_df
                         .groupBy("continent")
                         .agg(collect_list("name").alias("student_names"))
                         .withColumn("student_names", sort_array("student_names"))
                         .orderBy("continent"))

    student_data_array = (grouped_values_df
    .select(collect_list(col("student_names")).alias("student_names"))
    .collect()[0]["student_names"])

    max_rows = grouped_values_df.select(max(size(col("student_names"))).alias("max_rows")).collect()[0]["max_rows"]
    student_data_rows = [[None, ] * len(continent_columns) for _ in range(max_rows)]

    for contIdx, contValue in enumerate(student_data_array):
        for idx, value in enumerate(contValue):
            student_data_rows[idx][contIdx] = value

    student_data_rows = [tuple(elem) for elem in student_data_rows]

    final_df = spark.createDataFrame(student_data_rows).toDF(*continent_columns)
    final_df.show()
