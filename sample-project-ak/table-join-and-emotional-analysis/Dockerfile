FROM jupyter/pyspark-notebook:latest

USER root

# Εγκατάσταση Python packages
RUN pip install text2emotion psycopg2-binary emoji==1.6.3

# Κατέβασμα των απαραίτητων NLTK δεδομένων για text2emotion
RUN python -c "import nltk; nltk.download('wordnet'); nltk.download('omw-1.4'); nltk.download('punkt'); nltk.download('punkt_tab')"

# Προσθήκη PostgreSQL JDBC driver στον φάκελο των Spark jars
ADD https://jdbc.postgresql.org/download/postgresql-42.6.0.jar /usr/local/spark/jars/

# Αντιγραφή του Python αρχείου ανάλυσης
COPY analyze_emotions.py /home/jovyan/

# Ορισμός working directory
WORKDIR /home/jovyan/

# Εκκίνηση του Spark job
CMD ["spark-submit", "analyze_emotions.py"]
