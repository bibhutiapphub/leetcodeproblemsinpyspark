from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    investment_data = ((1, 10, 5, 10, 10)
                           , (2, 20, 20, 20, 20)
                           , (3, 10, 30, 20, 20)
                           , (4, 10, 40, 40, 40)
                           , (5, 30, 40, 30, 30)
                       )
    investment_df = spark.createDataFrame(investment_data).toDF("PID", "TIV_2015", "TIV_2016", "LAT", "LON")
    lat_lon_partition_window = Window.partitionBy("LAT","LON")
    row_num_partition_window = row_number().over(Window.partitionBy("LAT", "LON").orderBy("PID"))
    max_row_num_partition = max("row_num").over(lat_lon_partition_window)
    unique_location_df = (investment_df.withColumn("row_num",row_num_partition_window)
                            .withColumn("max_row_num",max_row_num_partition)
                            .where("max_row_num = 1")
                            .select("PID", "TIV_2015", "TIV_2016", "LAT", "LON"))

    final_df = (unique_location_df.alias("unique_loc_left")
                .join(unique_location_df.alias("unique_loc_right")
                      ,(col("unique_loc_left.TIV_2015") == col("unique_loc_right.TIV_2015"))
                      & (col("unique_loc_left.PID") != col("unique_loc_right.PID")),"inner")
                .select(sum("unique_loc_left.TIV_2016").alias("TIV_2016")))

    final_df.show()

