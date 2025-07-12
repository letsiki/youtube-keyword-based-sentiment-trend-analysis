import whisper
import sys
import os

# Configuration options

# ISO code για Ελληνικά,Αγγλικά,Γαλλικά,Γερμανικά,Ιταλικά: el,en,fr,de,it
input_language = "en"  
# translate_to_english = False  # Για μετάφραση True ή False

# Ελέγχει αν ο χρήστης έδωσε όνομα αρχείου .mp3 όταν εκτέλεσε το script
if len(sys.argv) < 2:
    print("Χρήση: python transcribe.py <audio_file>")
    sys.exit(1)

audio_path = sys.argv[1]

# Ελεγχει αν υπάρχει το αρχείο
if not os.path.exists(audio_path):
    print(f"To αρχείο {audio_path} δεν βρέθηκε.")
    sys.exit(1)

# Φόρτωση μοντέλου (μπορείς να αλλάξεις: "tiny", "base", "small", "medium", "large")
model = whisper.load_model("medium")

# Επεξεργασία με ορισμό γλώσσας ή μετάφραση
result = model.transcribe(
    audio_path,
    language=input_language,
    task="transcribe"
)

# Αποθήκευση αποτελέσματος
with open("transcription.txt", "w", encoding="utf-8") as f:
    f.write(result["text"])

print("Απομαγνητοφώνηση/μετάφραση ολοκληρώθηκε.")
