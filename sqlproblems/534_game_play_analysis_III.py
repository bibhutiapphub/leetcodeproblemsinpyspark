from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()

    activity_data = [(1, 2, "2016-03-01", 5)
        , (1, 2, "2016-05-02", 6)
        , (1, 3, "2017-06-25", 1)
        , (3, 1, "2016-03-02", 0)
        , (3, 4, "2018-07-03", 5)]

    activity_df = (spark.createDataFrame(activity_data)
                   .toDF("player_id", "device_id", "event_date", "games_played")
                   .withColumn("event_date", to_date(col("event_date"), "yyyy-MM-dd")))

    activity_df.show()

    # Using Window functions
    cum_sum_window = (sum("games_played")
                      .over(Window
                            .partitionBy("player_id")
                            .orderBy(asc("event_date"))
                            .rowsBetween(Window.unboundedPreceding,Window.currentRow)))

    final_window_df = (activity_df.withColumn("games_played", cum_sum_window)
                       .select(col("player_id")
                               , col("games_played")
                               ))
    final_window_df.show()
