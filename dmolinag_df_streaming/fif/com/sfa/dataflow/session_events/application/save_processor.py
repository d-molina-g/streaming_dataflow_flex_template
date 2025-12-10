import apache_beam as beam
from typing import Dict, Any, List
import logging
import json
from datetime import datetime, date
from zoneinfo import ZoneInfo
from fif.com.sfa.dataflow.session_events.config import settings

logger = logging.getLogger(__name__)


class CleanQuoteDoFn(beam.DoFn):

    def __init__(self, schema_manager):
        self.schema_manager = schema_manager

        self.type_map = {
            'STRING': str,
            'INTEGER': int,
            'FLOAT': float,
            'NUMERIC': float,
            'BIGNUMERIC': float,
            'BOOLEAN': bool,
            'TIMESTAMP': str,
            'DATE': str,
            'DATETIME': str,
            'TIME': str,
            'BYTES': bytes,
        }

        self.fields_to_remove = ["event", "timestamp"]

        raw_encrypt_fields = settings.fieldsEncrypt or ""
        self.encryption_fields = [
            f.strip() for f in raw_encrypt_fields.split(",") if f.strip()
        ]
        self.key_field = settings.eventKeyField

    def start_bundle(self):
        if self.schema_manager._schema_cache is None:
            bq_schema = self.schema_manager.load_schema()
            logger.info(f"CleanQuote inicializado con {len(bq_schema)} campos de BigQuery")

    def safe_convert(self, value: Any, field_name: str, target_bq_type: str) -> Any:
        if value is None:
            return None

        if isinstance(value, str):
            stripped = value.strip().lower()
            if stripped in ("", "null", "none"):
                return None

        try:
            if target_bq_type == 'INTEGER':
                return int(float(value))

            elif target_bq_type in ('FLOAT', 'NUMERIC', 'BIGNUMERIC'):
                return float(value)

            elif target_bq_type == 'BOOLEAN':
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes', 'si', 'sí')
                return bool(value)

            elif target_bq_type == 'DATE':
                if isinstance(value, datetime):
                    return value.date().isoformat()
                if isinstance(value, date):
                    return value.isoformat()
                if isinstance(value, str):
                    base = value.split('T')[0].split(' ')[0]
                    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
                        try:
                            dt = datetime.strptime(base, fmt)
                            return dt.date().isoformat()
                        except ValueError:
                            continue
                    return base
                return str(value).split('T')[0].split(' ')[0]

            elif target_bq_type in ('TIMESTAMP', 'DATETIME'):
                if isinstance(value, datetime):
                    dt = value
                else:
                    s = str(value).strip()
                    iso_candidate = s.replace("Z", "+00:00")
                    dt = None
                    try:
                        dt = datetime.fromisoformat(iso_candidate)
                    except Exception:
                        patterns = [
                            "%Y-%m-%d %H:%M:%S",
                            "%Y-%m-%dT%H:%M:%S",
                            "%Y-%m-%dT%H:%M:%S.%f",
                            "%Y-%m-%d",
                            "%d-%m-%Y",
                            "%d/%m/%Y",
                            "%Y/%m/%d",
                        ]
                        for fmt in patterns:
                            try:
                                dt = datetime.strptime(s, fmt)
                                break
                            except ValueError:
                                continue
                    if dt is None:
                        logger.warning(
                            f"Campo '{field_name}' inválido '{value}', enviado como NULL"
                        )
                        return None

                # DATETIME — quitar timezone y la "T"
                if target_bq_type == 'DATETIME':
                    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")

                # TIMESTAMP — sí permite ISO estándar
                return dt.isoformat()

            elif target_bq_type == 'TIME':
                if isinstance(value, datetime):
                    return value.time().isoformat()
                return str(value)

            elif target_bq_type == 'STRING':
                return str(value)

            elif target_bq_type == 'BYTES':
                if isinstance(value, bytes):
                    return value
                return str(value).encode('utf-8')

            return str(value)

        except (ValueError, TypeError):
            logger.warning(
                f"Campo '{field_name}' con valor '{value}' no convertible a '{target_bq_type}'"
            )
            return "__FIELD_OMITTED__"

    def process(self, element: Any) -> List[Dict[str, Any]]:
        if element is None or element == "":
            logger.warning("Elemento vacío omitido")
            return

        if not isinstance(element, dict):
            try:
                if isinstance(element, (bytes, bytearray)):
                    element = element.decode("utf-8")
                element = json.loads(element)
                if not isinstance(element, dict):
                    raise ValueError("No es JSON objeto")
            except Exception as e:
                logger.warning(f"Formato inválido omitido. Error={e} | Elemento={element}")
                return

        key_value = element.get(self.key_field)
        if key_value in (None, "", "null", "Null", "NONE", "None"):
            logger.warning(f"Elemento omitido: '{self.key_field}' vacío. Elemento={element}")
            return

        bq_schema = self.schema_manager.get_schema()
        if not bq_schema:
            logger.error("No se pudo obtener el esquema de BigQuery")
            yield element
            return

        cleaned_element: Dict[str, Any] = {}

        for field_name, value in element.items():
            if field_name in self.fields_to_remove:
                continue

            if field_name not in bq_schema:
                continue

            target_bq_type = bq_schema[field_name]
            converted_value = self.safe_convert(value, field_name, target_bq_type)

            if converted_value == "__FIELD_OMITTED__":
                continue

            cleaned_element[field_name] = converted_value

        # Campos a encriptar
        for field in self.encryption_fields:
            if field in cleaned_element and cleaned_element[field] is not None:
                cleaned_element[field] = str(cleaned_element[field])

        # Validación key
        if self.key_field not in cleaned_element or cleaned_element[self.key_field] is None:
            logger.warning(
                f"Elemento omitido: campo clave '{self.key_field}' no convertido. Elemento={element}"
            )
            return

        # DATETIME correcto para BigQuery
        if "processDatetime" in bq_schema:
            now = datetime.now(ZoneInfo("America/Santiago"))
            cleaned_element["processDatetime"] = now.strftime("%Y-%m-%d %H:%M:%S.%f")

        yield cleaned_element
