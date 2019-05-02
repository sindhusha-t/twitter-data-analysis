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
#df.show()
# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("Technology")
sqlDF = spark.sql("SELECT COUNT(*) AS NumberOfTweets, 'Artificial Intelligence' as Language FROM Technology where text LIKE '%Artificial Intelligence%' or text like '%AI%'\
        UNION\
        SELECT COUNT(*) AS NumberOfTweets,'Internet of things' as Language FROM Technology where text LIKE '%Internet of things%' or text like '%IoT%'\
        UNION\
        SELECT COUNT(*) AS NumberOfTweets, 'Machine Learning' as Language FROM Technology where text LIKE '%Machine Learning%' or text like '%ML%'\
        UNION\
        SELECT COUNT(*) AS NumberOfTweets, 'Block Chain' as Language FROM Technology where text LIKE '%Block Chain%'\
         ")
pd = sqlDF.toPandas()
pd.to_csv('first.csv', index=False)
pd.plot.pie(y='NumberOfTweets',labels=['Artificial Intelligence', 'Internet of things', 'Machine Learning', 'Block Chain'],figsize=(5,5))
plt.show()
sqlDF.show()