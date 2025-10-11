from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    tree_data = [(1, None)
        , (2, 1)
        , (3, 1)
        , (4, 2)
        , (5, 2)]
    tree_df = spark.createDataFrame(tree_data).toDF("id", "p_id")
    joined_df = (tree_df.alias("tree_1")
                 .join(tree_df.alias("tree_2"), col("tree_1.id") == col("tree_2.p_id"), "left")
                 .withColumn("type", when((col("tree_1.p_id").isNull()), lit("Root"))
                             .when(col("tree_1.p_id").isNotNull() & (col("tree_1.id") == col("tree_2.p_id")),
                                   lit("Inner"))
                             .when(col("tree_2.id").isNull(), lit("Leaf"))
                             .otherwise(None))
                 .select(col("tree_1.id")
                         , col("type"))
                 .distinct()
                 .orderBy("id")
                 )
    joined_df.show()
