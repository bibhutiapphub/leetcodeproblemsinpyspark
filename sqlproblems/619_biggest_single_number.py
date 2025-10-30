from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    numbers_data = [(8,)
        , (8,)
        , (3,)
        , (3,)
        , (1,)
        , (4,)
        , (5,)
        , (6,)]

    numbers_df = (spark.createDataFrame(numbers_data)
                  .toDF("num"))
    final_df = numbers_df.groupBy("num").count().where("count = 1").select(max("num").alias("num"))
    final_df.show()
