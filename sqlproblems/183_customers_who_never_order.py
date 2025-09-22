from pyspark.sql import Window
from pyspark.sql.functions import *
from utils.spark_utils import *

spark = get_spark_session()
customer_data = [(1, "Joe"), (2, "Henry"), (3, "Sam"), (4, "Max")]
orders_data = [(1, 3), (2, 1)]

customers_df = spark.createDataFrame(customer_data).toDF("Id","Name")
orders_df = spark.createDataFrame(orders_data).toDF("Id","CustomerId")

join_cond = customers_df.Id == orders_df.CustomerId
customers_never_ordered_df = (customers_df.join(orders_df,join_cond,"left")
                              .where(orders_df.Id.isNull())
                              .select(customers_df.Name.alias("Customers")))
customers_never_ordered_df.show()