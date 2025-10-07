from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    my_data = [(0, 6), (1, 1), (2, 3), (3, 2)]
    frequency_df = (spark.createDataFrame(my_data)
                    .toDF("Number", "Frequency")
                    .withColumn("Frequency",col("Frequency").cast("int")))

    median_pos = (int(frequency_df
                      .select(sum("Frequency").alias("Total_numbers"))
                      .first()[0] / 2))

    row_num_window = row_number().over(Window.orderBy("num_series"))
    final_df = (frequency_df
                .withColumn("num_arr", array_repeat(col("Number"),col("Frequency")))
                .select(explode(col("num_arr")).alias("num_series"))
                .withColumn("row_num",row_num_window)
                .where(col("row_num").isin([median_pos,median_pos+1]))
                .select((sum(col("num_series"))/2).alias("median"))
                )

    final_df.show()