from utils.spark_utils import *

from pyspark.sql.functions import *

spark = get_spark_session()

my_data = [(123,"Bibhuti"),(245,"Siddhartha")]
my_df = spark.createDataFrame(my_data).toDF("id","name")
my_df.show()