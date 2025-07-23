import json
import logging
import dateparser
from datetime import datetime, time

import text2emotion as te
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType, TimestampType

logging.basicConfig(level=logging.ERROR)

# Start Spark
spark = (
    SparkSession.builder.appName("EmotionAnalysisAndSummarization")
    .master("local[*]")  # Use all cores
    .config("spark.executor.memory", "4g")  # Raise executor memory
    .config("spark.driver.memory", "4g")  # Raise driver memory
    .config("spark.default.parallelism", "16")  # More parallel tasks
    .config(
        "spark.sql.shuffle.partitions", "16"
    )  # Fewer, larger partitions for small datasets
    .getOrCreate()
)

# JDBC connection
jdbc_url = "jdbc:postgresql://data_warehouse:5432/project_data"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
}

# Load tables
comments_df = spark.read.jdbc(
    url=jdbc_url,
    table="netanyahu_plus_gaza_comments_table",
    properties=properties,
)
text_df = spark.read.jdbc(
    url=jdbc_url,
    table="netanyahu_plus_gaza_text_from_audio",
    properties=properties,
)


# === UDF analyze emotion ===
def analyze_emotion(comment):
    try:
        emotions = te.get_emotion(comment)
        dominant_emotion = max(emotions, key=emotions.get)
        return json.dumps(dominant_emotion)
    except Exception as e:
        return json.dumps({"error": str(e)})


emotion_udf = udf(analyze_emotion, StringType())


# === UDF: Text summarization (based on word count) ===
def summarize_text(text):
    try:
        if not text or len(text.strip().split()) < 30:
            return text
        parser = PlaintextParser.from_string(text, Tokenizer("english"))
        summarizer = LexRankSummarizer()
        summary = summarizer(parser.document, 3)
        return " ".join(str(sentence) for sentence in summary)
    except Exception as e:
        return f"SummaryError: {str(e)}"


summary_udf = udf(summarize_text, StringType())


# emotion_to_number
def map_emotion_to_number(emotion_str):
    try:
        if not emotion_str or not isinstance(emotion_str, str):
            return "Invalid input: not a string"
        emotion = emotion_str.strip('"')
        mapping = {
            "Happy": 3,
            "Surprise": 0,
            "Angry": -3,
            "Sad": -1,
            "Fear": -2,
        }
        return mapping.get(emotion, "Not Found")
    except Exception as e:
        logging.error(f"Exception in map_emotion_to_number: {e}")
        return str(e)


emotion_number_udf = udf(map_emotion_to_number, IntegerType())


# UDF για μετατροπή σχετικής ημερομηνίας σε timestamp
def parse_relative_date(published_at, inserted_at):
    if not published_at or not inserted_at:
        return "Invalid input in parse relative date"
    try:
        if not isinstance(inserted_at, datetime):
            inserted_at = datetime.combine(inserted_at, time.min)

        parsed_date = dateparser.parse(
            published_at,
            settings={
                "RELATIVE_BASE": inserted_at,
                "PREFER_DATES_FROM": "past",
            },
            languages=["el"],
        )

        return parsed_date
    except Exception as e:
        logging.error(f"Exception in parse_relative_date: {e}")
        return str(e)


# Τύπος εξόδου Timestamp
parse_relative_date_udf = udf(parse_relative_date, TimestampType())

comments_df = comments_df.withColumn(
    "published_at_absolute",
    parse_relative_date_udf(col("published_at"), col("inserted_at")),
)
# Join the two tables
joined_df = text_df.join(comments_df, on="video_id")

# Apply UDFs
final_df = (
    joined_df.withColumn("emotion", emotion_udf(col("comment")))
    .withColumn("text_field", summary_udf(col("text_field")))
    .withColumnRenamed("text_field", "video_summary")
    .withColumn("emotion_number", emotion_number_udf(col("emotion")))
)

# Register to new table
final_df.write.jdbc(
    url=jdbc_url,
    table="netanyahu_plus_gaza_text_with_summary_and_comments_with_emotion",
    mode="overwrite",
    properties=properties,
)
