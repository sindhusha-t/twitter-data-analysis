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
sqlDF = spark.sql("SELECT 'Horror' as Movies, count(*) as Count from Technology where text like '%horror%' and text like '%vfx%'\
        UNION\
        SELECT 'comedy' as Movies, count(*) as Count from Technology where text like '%comedy%' and text like '%vfx%'\
        UNION\
        SELECT 'thriller' as Movies, count(*) as count from Technology where text like '%thriller' and text like '%vfx%'\
        UNION\
        SELECT 'action' as Movies, count(*) as Count from technology where text like '%action%' and text like '%vfx%'")
plt.show()
pd = sqlDF.toPandas()
pd.to_csv('eight.csv', index=False)
pd.plot(kind="bar",x="Movies",y="Count")
sqlDF.show()