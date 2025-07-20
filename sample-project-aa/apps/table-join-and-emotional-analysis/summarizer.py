import psycopg2
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer  # Εναλλακτικά: LsaSummarizer, LuhnSummarizer

# === ΡΥΘΜΙΣΕΙΣ ΣΥΝΔΕΣΗΣ ===
DB_SETTINGS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "mydb",
    "user": "user",
    "password": "password"
}

# === ΠΕΡΙΛΗΨΗ ΜΕ sumy ===
def summarize_text(text, sentence_count=3):
    if not text or len(text.strip()) < 30:
        return text  # Αγνόησε μικρά ή άδεια κείμενα

    parser = PlaintextParser.from_string(text, Tokenizer("english"))
    summarizer = LexRankSummarizer()
    summary = summarizer(parser.document, sentence_count)
    return " ".join(str(sentence) for sentence in summary)

# === ΣΥΝΔΕΣΗ ΚΑΙ ΕΠΕΞΕΡΓΑΣΙΑ ===
def process_texts():
    try:
        conn = psycopg2.connect(**DB_SETTINGS)
        cursor = conn.cursor()

        # Επιλογή όλων των εγγραφών με κείμενο
        cursor.execute("SELECT id, text_field FROM text_from_audio")
        rows = cursor.fetchall()

        for row in rows:
            record_id, original_text = row
            summarized_text = summarize_text(original_text)

            # Ενημέρωση εγγραφής
            cursor.execute(
                "UPDATE text_from_audio SET text_field = %s WHERE id = %s",
                (summarized_text, record_id)
            )
            print(f"Updated record {record_id}")

        conn.commit()
        print("All records updated successfully.")

    except Exception as e:
        print("Error:", e)

    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    process_texts()
