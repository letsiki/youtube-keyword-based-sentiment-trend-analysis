import dateparser
from datetime import datetime

def main():
    text = "πριν από 5 ημέρες"
    base_date = datetime(2025, 7, 19)
    
    parsed_date = dateparser.parse(
        text,
        settings={
            "RELATIVE_BASE": base_date,
            "PREFER_DATES_FROM": "past"
        },
        languages=["el"]
    )
    
    if parsed_date:
        print(f"Parsed date for '{text}': {parsed_date}")
    else:
        print(f"Failed to parse '{text}'")

if __name__ == "__main__":
    main()
