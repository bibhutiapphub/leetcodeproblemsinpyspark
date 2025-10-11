from pyspark.sql import Window

from utils.spark_utils import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = get_spark_session()
    sales_person_data = ((1, "John", 100000, 6, "4/1/2006")
                             , (2, "Amy", 120000, 5, "5/1/2010")
                             , (3, "Mark", 65000, 12, "12/25/2008")
                             , (4, "Pam", 25000, 25, "1/1/2005")
                             , (5, "Alex", 50000, 10, "2/3/2007")
                         )

    company_data = ((1, "RED", "Boston")
                        , (2, "ORANGE", "New York")
                        , (3, "YELLOW", "Boston")
                        , (4, "GREEN", "Austin")
                    )

    order_data = ((1, "1/1/2014", 3, 4, 100000)
                      , (2, "2/1/2014", 4, 5, 5000)
                      , (3, "3/1/2014", 1, 1, 50000)
                      , (4, "4/1/2014", 1, 4, 25000)
                  )

    sales_person_df = (spark.createDataFrame(sales_person_data)
                       .toDF("sales_id", "name", "salary", "commission_rate", "hire_date")
                       .withColumn("hire_date", to_date(col("hire_date"), "M/d/yyyy")))
    company_df = spark.createDataFrame(company_data).toDF("com_id", "name", "city")
    order_df = (spark.createDataFrame(order_data)
                .toDF("order_id", "date", "com_id", "sales_id", "amount")
                .withColumn("date", to_date(col("date"), "M/d/yyyy")))

    sales_id_rows = order_df.where("com_id == 1").select("sales_id").collect()
    sales_id_list = [sales_id_row.sales_id for sales_id_row in sales_id_rows]
    joined_df = (sales_person_df
                 .where(~sales_person_df["sales_id"].isin(sales_id_list))
                 .select("name"))
    joined_df.show()
