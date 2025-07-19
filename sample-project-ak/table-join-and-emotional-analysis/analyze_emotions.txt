import text2emotion as te
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Start Spark
spark = SparkSession.builder \
    .appName("EmotionAnalysis") \
    .getOrCreate()

# JDBC connection
jdbc_url = "jdbc:postgresql://postgres-db:5432/mydb"
properties = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}

# Read tables
comments_df = spark.read.jdbc(url=jdbc_url, table="comments", properties=properties)
text_df = spark.read.jdbc(url=jdbc_url, table="text_from_audio", properties=properties)

# Join on video_id
joined_df = text_df.join(comments_df, on="video_id")

# Define UDF to analyze emotion for a single comment string
def analyze_emotion(comment):
    try:
        emotions = te.get_emotion(comment)
        dominant_emotion = max(emotions, key=emotions.get)
        return json.dumps(dominant_emotion)
    except Exception as e:
        return json.dumps({"error": str(e)})

emotion_udf = udf(analyze_emotion, StringType())

# Add new column with emotion analysis per comment
result_df = joined_df.withColumn("emotion_summary", emotion_udf(col("comment")))

# Write back to a new table to avoid overwriting original
result_df.write.jdbc(url=jdbc_url, table="comments_with_emotion", mode="overwrite", properties=properties)
