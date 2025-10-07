from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    candidate_data = [(1, "A")
        , (2, "B")
        , (3, "C")
        , (4, "D")
        , (5, "E")]

    vote_data = [(1, 2)
        , (2, 4)
        , (3, 3)
        , (4, 2)
        , (5, 5)]

    candidate_df = spark.createDataFrame(candidate_data).toDF("Id", "Name")
    vote_df = spark.createDataFrame(vote_data).toDF("Id", "CandidateId")

    join_cond = candidate_df.Id == vote_df.CandidateId
    final_df = (candidate_df
                .join(vote_df, join_cond, "inner")
                .groupBy("Name")
                .agg(count(vote_df.Id).alias("vote_count"))
                .withColumn("row_num",row_number().over(Window.orderBy(desc("vote_count"))))
                .where("row_num = 1")
                .select("Name")
                )
    final_df.show()
