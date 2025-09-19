from utils.spark_utils import *

spark = get_spark_session()

person_data = [(1, "Allen", "Wang"), (2, "Bob", "Alice")]
address_data = [(1, 2, "New York City", "New York"), (2, 3, "Leetcode", "California")]
person_df = spark.createDataFrame(person_data).toDF("personId", "firstName", "lastName")
address_df = spark.createDataFrame(address_data).toDF("addressId", "personId", "city", "state")
join_cond = person_df.personId == address_df.personId
final_df = (person_df.join(address_df,join_cond,"left")
                        .select("firstName"
                                ,"lastName"
                                ,"city"
                                ,"state"))
final_df.show()