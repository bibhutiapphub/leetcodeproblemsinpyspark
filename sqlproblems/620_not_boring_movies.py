from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    movies_data = [(1,"War","great 3D",8.9)
        , (2,"Science","fiction",8.5)
        , (3,"Irish","boring",6.2)
        , (4,"Ice Song","Fantacy",8.6)
        , (5,"House Card","Interesting",9.1)]

    movies_df = (spark.createDataFrame(movies_data)
                  .toDF("id","movie","description","rating"))

    final_df = movies_df.where(expr("id%2 != 0 and description != 'boring'"))
    final_df.show()
