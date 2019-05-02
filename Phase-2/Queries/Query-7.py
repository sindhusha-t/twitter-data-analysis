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
sqlDF = spark.sql("SELECT 'Stanford' as Colleges, count(*) as Count from Technology where text like '%Stanford%' and text like '%AI%'\
        UNION\
        SELECT 'MIT' as Colleges, count(*) as Count from Technology where text like '%MIT%' and text like '%AI%'\
        UNION\
        SELECT 'UMKC' as Colleges, count(*) as Count from Technology where text like '%UMKC%' or text like '%AI%'")
plt.show()
pd = sqlDF.toPandas()
pd.to_csv('sixth.csv', index=False)
pd.plot(kind="bar",x="Colleges",y="Count")
sqlDF.show()
