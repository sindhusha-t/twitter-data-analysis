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
sqlDF = spark.sql("SELECT COUNT(*) AS NumberOfTweets, 'HTML' as Language FROM Technology where text LIKE '%HTML%' UNION SELECT COUNT(*) AS NumberOfTweets,'Java' as Language FROM Technology where text LIKE '%java%' UNION SELECT COUNT(*) AS NumberOfTweets, 'python' as Language FROM Technology where text LIKE '%python%' UNION SELECT COUNT(*) AS NumberOfTweets, 'Angular' as Language FROM Technology where text LIKE '%angular%'")
pd = sqlDF.toPandas()
pd.to_csv('fourth.csv', index=False)
pd.plot.pie(y='NumberOfTweets',labels=['HTML', 'Java', 'python', 'Angular'],figsize=(5,5))
plt.show()
sqlDF.show()