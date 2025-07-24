import json
import logging
import dateparser
from datetime import datetime, time
from transformers import pipeline
import re


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
    url=jdbc_url, table="comments_table", properties=properties
)
text_df = spark.read.jdbc(
    url=jdbc_url, table="text_from_audio", properties=properties
)


emotion_classifier = pipeline(
    "text-classification",
    model="j-hartmann/emotion-english-distilroberta-base",
    return_all_scores=True,
)


# === UDF analyze emotion ===
def analyze_emotion(comment):
    try:
        # Run the classifier
        results = emotion_classifier(comment)[
            0
        ]  # [0] since pipeline returns a list of lists

        # Find the dominant emotion
        dominant = max(results, key=lambda x: x["score"])
        return dominant["label"]

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
        if not isinstance(emotion_str, str):
            return None  # Spark-friendly

        label = emotion_str.lower()

        mapping = {
            "joy": 3,
            "surprise": 0,
            "anger": -3,
            "sadness": -1,
            "fear": -2,
            "disgust": -2,
            "neutral": 0,
        }

        return mapping.get(label, None)
    except Exception as e:
        logging.error(f"Exception in map_emotion_to_number: {e}")
        return None


emotion_number_udf = udf(map_emotion_to_number, IntegerType())


# UDF για μετατροπή σχετικής ημερομηνίας σε timestamp
def parse_relative_date(published_at, inserted_at):
    if not published_at or not inserted_at:
        return "Invalid input in parse relative date"
    try:
        if not isinstance(inserted_at, datetime):
            inserted_at = datetime.combine(inserted_at, time.min)

        published_at = re.sub(r" \(τροποποιήθηκε\)", "", published_at)

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
    table="text_with_summary_and_comments_with_emotion",
    mode="overwrite",
    properties=properties,
)
