FROM jupyter/pyspark-notebook:latest

USER root

# Install required Python packages
RUN pip install \
    transformers \
    torch \
    psycopg2-binary \
    emoji==1.6.3 \
    dateparser \
    sumy

# Download necessary NLTK data for sumy
RUN python -c "import nltk; nltk.download('punkt')"

# Add PostgreSQL JDBC driver to Spark jars folder
ADD https://jdbc.postgresql.org/download/postgresql-42.6.0.jar /usr/local/spark/jars/

# Copy the Python analysis script into the container
COPY analyze_emotions.py /home/jovyan/

# Set working directory
WORKDIR /home/jovyan/

# Run the Spark job
CMD ["spark-submit", "analyze_emotions.py"]