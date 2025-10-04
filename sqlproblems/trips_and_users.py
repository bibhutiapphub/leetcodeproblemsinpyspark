from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()

    trips_data = [(1, 1, 10, 1, "completed", "2013-10-01")
        , (2, 2, 11, 1, "cancelled_by_driver", "2013-10-01")
        , (3, 3, 12, 6, "completed", "2013-10-01")
        , (4, 4, 13, 6, "cancelled_by_client", "2013-10-01")
        , (5, 1, 10, 1, "completed", "2013-10-02")
        , (6, 2, 11, 6, "completed", "2013-10-02")
        , (7, 3, 12, 6, "completed", "2013-10-02")
        , (8, 2, 12, 12, "completed", "2013-10-03")
        , (9, 3, 10, 12, "completed", "2013-10-03")
        , (10, 4, 13, 12, "cancelled_by_driver", "2013-10-03")]

    users_data = [(1, "No", "client")
        , (2, "Yes", "client")
        , (3, "No", "client")
        , (4, "No", "client")
        , (10, "No", "driver")
        , (11, "No", "driver")
        , (12, "No", "driver")
        , (13, "No", "driver")]

    trips_df = (spark.createDataFrame(trips_data)
                .toDF("id", "client_id", "driver_id", "city_id", "status", "request_at")
                .withColumn("request_at", to_date(col("request_at"), "yyyy-MM-dd")))
    users_df = (spark.createDataFrame(users_data)
                .toDF("user_id", "banned", "role"))

    join_cond = trips_df.client_id == users_df.user_id

    unbanned_clients_trips_df = (trips_df.join(users_df, join_cond, "inner")
                                 .where(users_df.banned == "No")
                                 .select(trips_df.status
                                         , trips_df.request_at)
                                 .orderBy("request_at"))

    final_df = (unbanned_clients_trips_df
                .withColumn("status_transformed", when(col("status") == "completed", 0).otherwise(1))
                .groupby("request_at")
                .agg(round(avg("status_transformed"), 2).alias("Cancellation_Rate"))
                .select(col("request_at").alias("Day")
                        , col("Cancellation_Rate"))
                .orderBy("Day"))
    final_df.show()
