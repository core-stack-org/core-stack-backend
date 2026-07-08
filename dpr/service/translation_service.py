import json
from pathlib import Path
from django.conf import settings

TRANSLATION_DIR = Path(settings.BASE_DIR) / "data" / "dpr" / "translations"


def load_translations(language="en"):

    file_path = TRANSLATION_DIR / f"{language}.json"

    if not file_path.exists():
        file_path = TRANSLATION_DIR / "en.json"

    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)
