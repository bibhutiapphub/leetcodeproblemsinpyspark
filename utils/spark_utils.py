from pyspark.sql import SparkSession

def get_spark_session():
    spark = (SparkSession.builder
             .appName("LeetcodeProblems")
             .master("local[2]")
             .enableHiveSupport()
             .getOrCreate())

    return spark
