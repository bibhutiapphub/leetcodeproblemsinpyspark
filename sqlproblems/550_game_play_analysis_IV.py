from pyspark.sql import Window
from pyspark.sql.functions import date_diff

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()

    activity_data = [(1, 2, "2016-03-01", 5)
        , (1, 2, "2016-03-02", 6)
        , (2, 3, "2017-06-25", 1)
        , (3, 1, "2016-03-02", 0)
        , (3, 4, "2018-07-03", 5)]

    activity_df = (spark.createDataFrame(activity_data)
                   .toDF("player_id", "device_id", "event_date", "games_played")
                   .withColumn("event_date", to_date(col("event_date"), "yyyy-MM-dd")))

    # Using Window functions
    lag_event_date_window = lag("event_date").over(Window.partitionBy("player_id").orderBy(asc("event_date")))
    row_num_window = row_number().over(Window.partitionBy("player_id").orderBy(desc("is_consecutive_days_played")))
    final_window_df = (activity_df
                       .withColumn("is_consecutive_days_played",
                                   when(date_diff(col("event_date"), lag_event_date_window) == 1, 1)
                                   .otherwise(0))
                       .withColumn("row_num", row_num_window)
                       .where("row_num = 1")
                       .select(round(avg("is_consecutive_days_played"),2).alias("fraction"))
                       )
    final_window_df.show()
