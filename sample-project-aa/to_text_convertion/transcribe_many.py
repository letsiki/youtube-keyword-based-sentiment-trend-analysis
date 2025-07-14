import whisper
import argparse
import os
import pickle
from pathlib import Path

# input_language = "en"  # ISO code π.χ. el, en, fr, de, it
# translate_to_english = False  # Θες μετάφραση στα Αγγλικά; True ή False
# model_size = "medium"  # Επιλογή μοντέλου: tiny, base, small, medium, large

# Argument parsing
parser = argparse.ArgumentParser(
    description="Απομαγνητοφώνηση αρχείων ήχου με Whisper"
)

parser.add_argument(
    "--urls", nargs="+", help="Λίστα από αρχεία ήχου για επεξεργασία"
)
# ISO code π.χ. el, en, fr, de, it
parser.add_argument(
    "--lang", default="en", help="Γλώσσα εισόδου (ISO code)"
)
# Επιλογή μοντέλου: tiny, base, small, medium, large
parser.add_argument(
    "--model",
    default="medium",
    help="Μοντέλο whisper (tiny/base/small/medium/large)",
)

args = parser.parse_args()

# base path
base_path = Path("/app")

# mp3 Path
mp3_path = base_path / "mp3"

# text Path
text_path = base_path / "text"

# Load model μία φορά
print(f"Φόρτωση μοντέλου Whisper ({args.model})...")
model = whisper.load_model(args.model)

# convert video_ids to mp3's
audio_paths = [
    mp3_path / (video_id[1:] + ".mp3") for video_id in args.urls
]


# Επεξεργασία πολλών αρχείων
for audio_path in audio_paths:
    if not os.path.exists(audio_path):
        print(f"To αρχείο {audio_path} δεν βρέθηκε. Παράλειψη.")
        continue

    print(f"Επεξεργασία: {audio_path}")
    result = model.transcribe(
        str(audio_path),
        language=args.lang,
        task="transcribe",
        fp16=False,  # "translate" if translate_to_english else "transcribe",
    )

    # Δημιουργία output filename
    base_name = os.path.splitext(os.path.basename(audio_path))[0]
    output_file = f"{base_name}.txt"

    with open(text_path / output_file, "w", encoding="utf-8") as f:
        f.write(result["text"])

    print(f"Αποθηκεύτηκε: {output_file}")

print("Ολοκληρώθηκε η απομαγνητοφώνηση/μετάφραση όλων των αρχείων.")

# os.makedirs("/airflow/xcom", exist_ok=True)
# with open("/airflow/xcom/return.pkl", "wb") as f:
#     pickle.dump(list(urls), f)
