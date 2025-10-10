from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    request_accepted_data = ((1, 2, "2016-06-03"),
                             (1, 3, "2016-06-08"),
                             (2, 3, "2016-06-08"),
                             (3, 4, "2016-06-09")
                             )
    request_accepted_df = (spark
                           .createDataFrame(request_accepted_data)
                           .toDF("requester_id", "accepter_id", "accept_date")
                           .withColumn("accept_date", col("accept_date").cast("date")))

    requester_counts_df = request_accepted_df.groupby("requester_id").count().withColumnRenamed("count",
                                                                                                "requester_count")
    accepter_counts_df = request_accepted_df.groupby("accepter_id").count().withColumnRenamed("count", "accepter_count")

    rank_num_friends_partition = dense_rank().over(Window.orderBy(desc("num")))

    joined_df = (requester_counts_df.join(accepter_counts_df,
                                          requester_counts_df.requester_id == accepter_counts_df.accepter_id,
                                          "fullouter")
                 .withColumn("id", when(col("requester_id").isNull(), col("accepter_id"))
                             .when(col("accepter_id").isNull(), col("requester_id"))
                             .otherwise(col("requester_id")))

                 .withColumn("num", when(col("requester_id").isNull(), col("accepter_count"))
                             .when(col("accepter_id").isNull(), col("requester_count"))
                             .otherwise(col("requester_count") + col("accepter_count")))
                 .withColumn("rank_friends_num", rank_num_friends_partition)
                 .where("rank_friends_num == 1")
                 .select("id", "num")
                 )
    request_accepted_df.show()
    joined_df.show()
