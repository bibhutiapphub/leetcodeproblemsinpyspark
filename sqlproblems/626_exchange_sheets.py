from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    movies_data = [(1, "Abbot")
        , (2, "Doris")
        , (3, "Emerson")
        , (4, "Green")
        , (5, "Jeames")
        , (6, "Caesar")]

    student_df = (spark.createDataFrame(movies_data)
                  .toDF("id", "student"))

    student_df.show()

    next_seat_student = lead("student").over(Window.orderBy("id"))
    prev_seat_student = lag("student").over(Window.orderBy("id"))

    prev_next_student_df = (student_df.withColumn("prev_student", prev_seat_student)
                            .withColumn("next_student", next_seat_student)
                            .select(col("id").alias("student_id")
                                    , col("prev_student")
                                    , col("next_student")))

    final_df = (student_df
                .join(prev_next_student_df, student_df["id"] == prev_next_student_df["student_id"], "left")
                .withColumn("student",
                            when(((col("id") % 2 == 1) & (col("next_student").isNotNull())), col("next_student"))
                            .when(col("id") % 2 == 0, col("prev_student"))
                            .otherwise(col("student")))
                .select(col("id")
                        , col("student"))
                .orderBy("id")
                )
    final_df.show()
