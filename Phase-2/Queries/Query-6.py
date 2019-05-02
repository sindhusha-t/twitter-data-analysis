from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import matplotlib as plt
import matplotlib.pyplot as plt
import pandas as pd
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
# spark is an existing SparkSession
df = spark.read.json("/home/siri/Downloads/project/tech_tweets.json")
# Displays the content of the DataFrame to stdout

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("Technology")

sqlDF = spark.sql("SELECT 'Home' as Field, count(*) as Count from Technology where text like '%home%' and (text like '%AI%' or text like '%IoT%' or text like '%ML%')\
        UNION\
        SELECT 'Business' as Field, count(*) as Count from Technology where text like '%business%' and (text like '%AI%' or text like '%IoT%' or text like '%ML%')\
        UNION\
        SELECT 'Health' as Field, count(*) as Count from Technology where text like '%health%' and (text like '%AI%' or text like '%IoT%' or text like '%ML%')")
        
pd = sqlDF.toPandas()
pd.to_csv('sixth.csv', index=False)
pd.plot(kind="bar",x="Field",y="Count")
plt.show()
sqlDF.show()