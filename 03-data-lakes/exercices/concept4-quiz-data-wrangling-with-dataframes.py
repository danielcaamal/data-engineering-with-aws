# # Data Wrangling with DataFrames Coding Quiz

from pyspark.sql import SparkSession

# TODOS: 
# 1) import any other libraries you might need
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.window import Window
import datetime
import numpy as np
import pandas as pd

# 2) instantiate a Spark session 
spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()

# 3) read in the data set located at the path "../../data/sparkify_log_small.json"
path = "/workspace/home/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/lesson-2-spark-essentials/exercises/data/sparkify_log_small.json"
sparkify_df = spark.read.json(path)

# 4) write code to answer the quiz questions 
sparkify_df.printSchema()

# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?
all_pages = sparkify_df.select('page').distinct()

visited_pages = sparkify_df \
    .where(sparkify_df.userId == '') \
    .select('page').distinct()

not_visited_pages = all_pages.subtract(visited_pages)

not_visited_pages.show()

# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
# 

# Register the Spark DataFrame as a temporary view
sparkify_df.createOrReplaceTempView("sparkify_view")

# SQL query to group users by empty userId and non-empty userId
query = """
    SELECT
        CASE WHEN userId = '' THEN 'Empty userId' ELSE 'Non-empty userId' END AS userId_group,
        COUNT(DISTINCT status) AS distinct_status,
        COUNT(DISTINCT registration) AS distinct_registration,
        COUNT(DISTINCT auth) AS distinct_auth
    FROM
        sparkify_view
    GROUP BY
        CASE WHEN userId = '' THEN 'Empty userId' ELSE 'Non-empty userId' END
"""

result = spark.sql(query)
result.show()

queryRegistration = """
    SELECT 
        DISTINCT registration
    FROM 
        sparkify_view
    WHERE
        userId = '' 
"""
result = spark.sql(queryRegistration)
result.show()


# # Question 3
# 
# How many female users do we have in the data set?
countFemales = sparkify_df \
    .where(sparkify_df.gender == 'F') \
    .select('userId', 'gender') \
    .dropDuplicates() \
    .count()

print(countFemales)


# # Question 4
# 
# How many songs were played from the most played artist?

sparkify_df.filter(sparkify_df.page == 'NextSong') \
    .select('Artist') \
    .groupBy('Artist') \
    .agg({'Artist':'count'}) \
    .withColumnRenamed('count(Artist)', 'Playcount') \
    .sort(desc('Playcount')) \
    .show(1)

# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
# 

user_window = Window \
    .partitionBy('userID') \
    .orderBy(desc('ts')) \
    .rangeBetween(Window.unboundedPreceding, 0)

ishome = udf(lambda ishome : int(ishome == 'Home'), IntegerType())

# Filter only NextSong and Home pages, add 1 for each time they visit Home
# Adding a column called period which is a specific interval between Home visits
cusum = sparkify_df.filter((sparkify_df.page == 'NextSong') | (sparkify_df.page == 'Home')) \
    .select('userID', 'page', 'ts') \
    .withColumn('homevisit', ishome(col('page'))) \
    .withColumn('period', Fsum('homevisit') \
    .over(user_window)) 
    
# This will only show 'Home' in the first several rows due to default sorting

cusum.show(300)


# See how many songs were listened to on average during each period
cusum.filter((cusum.page == 'NextSong')) \
    .groupBy('userID', 'period') \
    .agg({'period':'count'}) \
    .agg({'count(period)':'avg'}) \
    .show()



