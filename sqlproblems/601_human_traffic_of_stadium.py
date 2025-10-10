from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    stadium_data = ((1, "2017-01-01", 10),
                    (2, "2017-01-02", 109),
                    (3, "2017-01-03", 150),
                    (4, "2017-01-04", 99),
                    (5, "2017-01-05", 145),
                    (6, "2017-01-06", 1455),
                    (7, "2017-01-07", 199),
                    (8, "2017-01-09", 188)
                    )
    stadium_df = (spark
                  .createDataFrame(stadium_data)
                  .toDF("id", "visit_date", "people")
                  .withColumn("visit_date",col("visit_date").cast("date")))

    intermediate_df = (stadium_df.alias("std_df_1")
                       .join(stadium_df.alias("std_df_2"), expr("std_df_1.id == std_df_2.id+1"))
                       .join(stadium_df.alias("std_df_3"), expr("std_df_1.id == std_df_3.id+2"))
                       .withColumn("flag", when(
                                                (col("std_df_1.people") >= 100)
                                                & (col("std_df_2.people") >= 100)
                                                & (col("std_df_3.people") >= 100), lit(1)
                                            ).otherwise(lit(0)))
                       .select(col("std_df_1.id").alias("id_1")
                               , col("std_df_1.people").alias("people_1")
                               , col("std_df_2.id").alias("id_2")
                               , col("std_df_2.people").alias("people_2")
                               , col("std_df_3.id").alias("id_3")
                               , col("std_df_3.people").alias("people_3")
                               , col("flag"))
                       )

    join_cond = ((stadium_df.id == intermediate_df.id_1)
                 | (stadium_df.id == intermediate_df.id_2)
                 | (stadium_df.id == intermediate_df.id_3))

    final_df = (stadium_df
                .join(intermediate_df, join_cond)
                .where("flag=1")
                .select("id"
                        ,"visit_date"
                        ,"people")
                .distinct()
                .orderBy("visit_date")
                )
    stadium_df.printSchema()
    final_df.show()
