from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import matplotlib as plt
import matplotlib.pyplot as plt
import pandas as pd
spark = SparkSession \
    .builder \
    .appName("apple Spark SQL basic example") \
    .getOrCreate()
# spark is an existing SparkSession
df = spark.read.json("/home/siri/Downloads/project/tech_tweets.json")
# Displays the content of the DataFrame to stdout
#df.show()
# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("Technology")
sqlDF = spark.sql("SELECT COUNT(*) AS NumberOfTweets, 'samsung' as Language FROM Technology where text LIKE '%samsung%'\
        UNION\
        SELECT COUNT(*) AS NumberOfTweets,'Android' as Language FROM Technology where text LIKE '%Android%'\
        UNION\
        SELECT COUNT(*) AS NumberOfTweets, 'apple' as Language FROM Technology where text LIKE '%apple%'\
        UNION\
        SELECT COUNT(*) AS NumberOfTweets, 'Lenovo' as Language FROM Technology where text LIKE '%Lenovo%'\
         ")
pd = sqlDF.toPandas()
pd.to_csv('fifth.csv', index=False)
pd.plot.pie(y='NumberOfTweets',labels=['samsung', 'Android', 'apple', 'Lenovo'],figsize=(5,5))
plt.show()
sqlDF.show()