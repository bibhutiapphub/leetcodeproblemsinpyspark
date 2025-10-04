from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()

    activity_data = [(1, 2, "2016-03-01", 5)
        , (1, 2, "2016-05-02", 6)
        , (2, 3, "2017-06-25", 1)
        , (3, 1, "2016-03-02", 0)
        , (3, 4, "2018-07-03", 5)]

    activity_df = (spark.createDataFrame(activity_data)
                   .toDF("player_id","device_id","event_date","games_played")
                   .withColumn("event_date",to_date(col("event_date"),"yyyy-MM-dd")))

    # Using groupBy  aggregate function
    final_group_by_df =  (activity_df
                          .groupBy("player_id")
                          .agg(min("event_date").alias("first_login"))
                          .orderBy("player_id"))
    final_group_by_df.show()

    # Using Window functions
    row_num_window = row_number().over(Window.partitionBy("player_id").orderBy(asc("event_date")))
    final_window_df = (activity_df.withColumn("row_num",row_num_window)
                       .where("row_num == 1")
                       .select(col("player_id")
                               ,col("event_date").alias("first_login")
                               )
                       )

    final_window_df.show()