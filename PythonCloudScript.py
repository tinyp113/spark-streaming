#Import all libraries

import findspark

# initialize your spark directory
findspark.init('/home/crom/spark')


# May cause deprecation warnings, safe to ignore, they aren't errors
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import desc
import re
import time
from textblob import TextBlob
from collections import namedtuple
import boto3
from io import StringIO
import datetime
import awscredentials
import pandas
# Creating the Spark Context
sc = SparkContext(master="local[2]", appName="TwitterStreaming")
sc.setLogLevel("ERROR")

#creating the streaming context
ssc = StreamingContext(sc, 2)
ssc.checkpoint("checkpoint")

#creating the SQL context
sqlContext = SQLContext(sc)


lines = ssc.socketTextStream("localhost", 5555)


#Function to clean tweet
def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", str(tweet)).split())


#Polarity analysis of a tweet
def analyze_sentiment_polarity(tweet):
    analysis = TextBlob(clean_tweet(tweet))
    # return str(analysis.sentiment.polarity)
    if analysis.sentiment.polarity > 0:
        return 1
    elif analysis.sentiment.polarity == 0:
        return 0
    else:
        return -1

#subjectivity analysis of a tweet
def analyze_sentiment_subjectivity(tweet):
    analysis = TextBlob(clean_tweet(tweet))
    return str(analysis.sentiment.subjectivity)
def file_len(i):
    with open("/home/crom/tweet.csv") as f:
        for  l in  enumerate(f):
            i+=1
    return i 
words = lines.flatMap(lambda text: text.split(" ")).filter(lambda text: text.lower().startswith('#'))
words.countByValueAndWindow(180, 60).map(lambda p: Tagscount(p[0], p[1])).foreachRDD(
    lambda rdd: rdd.toDF().sort(desc('count')).limit(10).registerTempTable("hashtags"))



response = input("Do you want to start the twitter stream ? Y/N (Please execute your stream listener in another terminal session and then put Y)\n")
ts = datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
save_file = open("/home/crom/tweet.csv","a")

if response == "Y":
    ssc.start()
    print("Session Started.....")
    print("Collecting tweets...waiting for 10 seconds..")
    print(words)
    
    filename = str( ts + save_file.name )
    while True:
        if (file_len(0) < 101):
            print ("Waiting for collect enough 100 tweets. " + file_len(0) +"/100 collected")

        elif (file_len(0) == 101):
            print("Uploading to S3 - Filename: " + filename + "\n")
            print("Connecting to AWS Boto3 session...Uploading the csv file")
            session = boto3.Session(awscredentials.aws_access_key_id, awscredentials.aws_secret_access_key)
            s3 = session.resource('s3')
            s3.Object('sparkstreamingcrom', filename).upload_file("/home/crom/tweet.csv")
            break
            #Connecting to a boto3 session - Update file in AWS Cloud S3 Bucket

    ssc.awaitTermination()

else:
    print("You ended the program")
    exit()

#pxx
