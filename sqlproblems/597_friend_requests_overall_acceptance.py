from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    friend_request = ((1, 2, "2016-06-01")
                          , (1, 3, "2016-06-01")
                          , (1, 4, "2016-06-01")
                          , (2, 3, "2016-06-02")
                          , (3, 4, "2016-06-09"))

    request_accepted = ((1, 2, "2016-06-03")
                            , (1, 3, "2016-06-08")
                            , (2, 3, "2016-06-08")
                            , (3, 4, "2016-06-09")
                            , (3, 4, "2016-06-10"))

    friend_request_df = (spark
                         .createDataFrame(friend_request)
                         .toDF("sender_id", "send_to_id", "request_date")
                         .withColumn("request_date", col("request_date").cast("date")))
    request_accepted_df = (spark
                           .createDataFrame(request_accepted)
                           .toDF("requester_id", "accepter_id", "accept_date")
                           .withColumn("accept_date", col("accept_date").cast("date"))
                           .select(
        round((count_distinct("requester_id", "accepter_id") / count("requester_id")), 2).alias("accept_rate")))

    request_accepted_df.show()
