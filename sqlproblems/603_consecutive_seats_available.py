from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    seats_data = ((1, 1),
                  (2, 0),
                  (3, 1),
                  (4, 1),
                  (5, 1),
                  (6, 0),
                  (7, 1),
                  (8, 1)
                  )

    next_seat = lead("free").over(Window.orderBy("seat_id"))
    prev_seat = lag("free").over(Window.orderBy("seat_id"))
    seats_df = spark.createDataFrame(seats_data).toDF("seat_id", "free")

    final_df = (seats_df
                .withColumn("next_seat", next_seat)
                .withColumn("prev_seat", prev_seat)
                .withColumn("consecutive_flag", when(
        (col("free") == 1) & ((col("free") == col("prev_seat")) | (col("free") == col("next_seat"))), lit(1))
                            .otherwise(lit(0)))
                .where(col("consecutive_flag") == 1)
                .select("seat_id")
                )

    # Alternative approach
    consecutive_seats_df = (seats_df.alias("seats_1")
                            .join(seats_df.alias("seats_2"), expr("seats_1.seat_id == seats_2.seat_id+1"), "inner")
                            .where("seats_1.free == seats_2.free")
                            .select(col("seats_1.seat_id").alias("seats_1_id")
                                    , col("seats_1.free").alias("seats_1_free")
                                    , col("seats_2.seat_id").alias("seats_2_id")
                                    , col("seats_2.free").alias("seats_2_free"))
                            )

    second_final_df = (seats_df
                       .join(consecutive_seats_df,(seats_df.seat_id == consecutive_seats_df.seats_1_id) | (seats_df.seat_id == consecutive_seats_df.seats_2_id))
                       .select("seat_id")
                       .distinct()
                       .orderBy("seat_id")
                       )
    second_final_df.show()
