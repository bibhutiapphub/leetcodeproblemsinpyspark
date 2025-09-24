from pyspark.sql import Window
from pyspark.sql.functions import *
from utils.spark_utils import *

spark = get_spark_session()

my_data = [(1,"2015-01-01",10)
            ,(2,"2015-01-02",25)
            ,(3,"2015-01-03",20)
            ,(4,"2015-01-04",30)]

lagTempValue = lag("temperature").over(Window.orderBy("recordDate"))

my_df = (spark.createDataFrame(my_data)
         .toDF("id","recordDate","temperature")
         .withColumn("recordDate",to_date(col("recordDate"),"yyyy-MM-dd"))
         .withColumn("lagTemprature",lagTempValue)
         .where(col("temperature") > col("lagTemprature"))
         .select("id"))


my_df.show()