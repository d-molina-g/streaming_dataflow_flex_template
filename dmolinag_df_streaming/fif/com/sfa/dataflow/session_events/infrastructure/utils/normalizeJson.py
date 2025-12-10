import json
import re
from typing import Union

def clean_json_nulls_to_empty(json_input: Union[str, dict, bytes]) -> str:

    if isinstance(json_input, dict):
        json_text = json.dumps(json_input, ensure_ascii=False)
    elif isinstance(json_input, (bytes, bytearray)):
        json_text = json_input.decode("utf-8")
    else:
        json_text = str(json_input)

    json_text = re.sub(r'\bNULL\b', 'null', json_text, flags=re.IGNORECASE)
    try:
        data = json.loads(json_text)
    except json.JSONDecodeError:
        raise ValueError("Error: el texto no es un JSON válido")

    def replace_null_values(value):
        if isinstance(value, dict):

            return {k: replace_null_values(v) for k, v in value.items()}
        elif isinstance(value, list):

            return [replace_null_values(v) for v in value]
        elif value is None or (isinstance(value, str) and value.strip().lower() == "null"):
            return ""
        else:
            return value

    cleaned_data = replace_null_values(data)
    cleaned_json_text = json.dumps(cleaned_data, ensure_ascii=False, indent=2)
    return cleaned_json_text
