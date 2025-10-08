from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    survey_data = [(5, "show", 285, None, 1, 123)
                    , (5, "answer", 285, 124124, 1, 124)
                    , (5, "show", 369, None, 2, 125)
                    , (5, "skip", 369, None, 2, 126)
                   ]

    survey_df = (spark.createDataFrame(survey_data)
                 .toDF("uid","action","question_id","answer_id","q_num","timestamp")
                 .select("action","question_id","answer_id")
                 .withColumn("show_flag",
                             when(col("action") == "show", lit(1))
                             .otherwise(lit(0)))
                 .withColumn("answer_flag",
                             when(col("action") == "answer", lit(1))
                             .otherwise(lit(0)))
                 .groupBy("question_id")
                 .agg(sum("show_flag").alias("show_count")
                      ,sum("answer_flag").alias("answer_count"))
                 .withColumn("answer_rate",col("answer_count")/col("show_count"))
                 .orderBy(desc("answer_rate"))
                 .select(col("question_id").alias("survey_log"))
                 .limit(1)
                 )

    survey_df.show()