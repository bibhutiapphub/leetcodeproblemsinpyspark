from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    follower_followee_data = (("A", "B")
                                  , ("B", "C")
                                  , ("B", "D")
                                  , ("D", "E")
                              )

    follower_followee_df = spark.createDataFrame(follower_followee_data).toDF("followee", "follower")

    joined_df = (follower_followee_df.alias("follower_df")
                 .join(follower_followee_df.alias("followee_df"), expr("follower_df.follower == followee_df.followee"))
                 .groupby(expr("follower_df.follower").alias("follower"))
                 .agg(count(expr("followee_df.follower")).alias("num"))
                 )
    joined_df.show()
