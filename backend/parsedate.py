from datetime import datetime

def parse_date(string):
    for fmt in ("%Y %b", "%Y"):
        try:
            return datetime.strptime(string, fmt).date()
        except Exception:
            pass