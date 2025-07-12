import whisper
import argparse
import os

#input_language = "en"  # ISO code π.χ. el, en, fr, de, it
#translate_to_english = False  # Θες μετάφραση στα Αγγλικά; True ή False
#model_size = "medium"  # Επιλογή μοντέλου: tiny, base, small, medium, large

# Argument parsing
parser = argparse.ArgumentParser(description="Απομαγνητοφώνηση αρχείων ήχου με Whisper")

parser.add_argument("audio_files", nargs="+", help="Λίστα από αρχεία ήχου για επεξεργασία")
# ISO code π.χ. el, en, fr, de, it
parser.add_argument("--lang", default="en", help="Γλώσσα εισόδου (ISO code)")
# Επιλογή μοντέλου: tiny, base, small, medium, large
parser.add_argument("--model", default="medium", help="Μοντέλο whisper (tiny/base/small/medium/large)")

args = parser.parse_args()

# Load model μία φορά
print(f"Φόρτωση μοντέλου Whisper ({args.model})...")
model = whisper.load_model(args.model)

# Επεξεργασία πολλών αρχείων
for audio_path in args.audio_files:
    if not os.path.exists(audio_path):
        print(f"To αρχείο {audio_path} δεν βρέθηκε. Παράλειψη.")
        continue

    print(f"Επεξεργασία: {audio_path}")
    result = model.transcribe(
        audio_path,
        language=args.lang,
        task="transcribe" # "translate" if translate_to_english else "transcribe"
    )

    # Δημιουργία output filename
    base_name = os.path.splitext(os.path.basename(audio_path))[0]
    output_file = f"{base_name}.txt"

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(result["text"])

    print(f"Αποθηκεύτηκε: {output_file}")

print("Ολοκληρώθηκε η απομαγνητοφώνηση/μετάφραση όλων των αρχείων.")
