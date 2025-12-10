import apache_beam as beam
import json
import logging
from apache_beam.transforms import window
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
import typing

# Importaciones
from fif.com.sfa.dataflow.session_events.infrastructure.adapters.pubsub_adapter import PubSubAdapter
from fif.com.sfa.dataflow.session_events.application.crypto_execute import crypto_execute
from fif.com.sfa.dataflow.session_events.application.save_processor import CleanQuoteDoFn
from fif.com.sfa.dataflow.session_events.application.schema_manager import SchemaManager
from fif.com.sfa.dataflow.session_events.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class MergeEventsFn(beam.CombineFn):
    def create_accumulator(self):
        return {}

    def add_input(self, acc, element):
        for k, v in element.items():
            if v not in (None, "", "null"):
                acc[k] = v
            else:
                acc.setdefault(k, None)
        return acc

    def merge_accumulators(self, accs):
        merged = {}
        for acc in accs:
            for k, v in acc.items():
                if v not in (None, "", "null"):
                    merged[k] = v
                else:
                    merged.setdefault(k, None)
        return merged

    def extract_output(self, acc):
        return acc

class ExtractKeyValueFn(beam.DoFn):
    def process(self, record):
        if record is None:
            logger.warning("Registro None recibido en ExtractKeyValueFn. Omitiendo.")
            return

        original_record = record

        # Si viene como bytes/bytearray, intentar decodificar y parsear JSON
        if isinstance(record, (bytes, bytearray)):
            try:
                text = record.decode("utf-8")
                record = json.loads(text)
            except Exception as e:
                logger.warning(
                    f" Registro bytes no se pudo parsear como JSON. "
                    f"Error={e}, Valor={original_record}"
                )
                return

        # Si viene como string, intentar parsear JSON
        if isinstance(record, str):
            try:
                record = json.loads(record)
            except Exception as e:
                logger.warning(
                    f" Registro string no se pudo parsear como JSON. "
                    f"Error={e}, Valor={original_record}"
                )
                return

        if isinstance(record, tuple) and len(record) == 2 and isinstance(record[1], dict):
            record = record[1]

        # Comprobación final del tipo
        if not isinstance(record, dict):
            logger.warning(
                f"Registro no es correcto. Tipo={type(record)}, Valor={original_record}. Omitiendo."
            )
            return

        key_field = settings.eventKeyField
        key_value = record.get(key_field)

        if key_value is None:
            logger.warning(
                f"Registro sin campo de clave primaria '{key_field}'. Elemento={record}. Omitiendo."
            )
            return

        yield (key_value, record)


class LogMergedFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        key, merged = element

        if not merged or len(merged) == 0:
            return

        try:
            start_str = window.start.to_utc_datetime().isoformat()
            end_str = window.end.to_utc_datetime().isoformat()
        except Exception:
            start_str = 'N/A'
            end_str = 'N/A'

        logger.info(
            f"[{start_str} - {end_str}] quoteId={key} evento fusionado OK"
        )
        yield merged

class CleanMerged(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | "Log Merged Data" >> beam.ParDo(LogMergedFn())


class EncryptFieldsFn(beam.DoFn):
    def __init__(self, fields, cipher_adapter, cipher_service, charset="utf-8"):
        self.fields = fields
        self.cipher_adapter = cipher_adapter
        self.cipher_service = cipher_service
        self.charset = charset

    def process(self, element):
        if not element:
            return
        try:
            encrypted_json = crypto_execute(
                self.cipher_adapter,
                self.cipher_service,
                element,
                "encrypt",
                self.fields,
                self.charset
            )
            yield encrypted_json
        except Exception as e:
            logger.error(f"Error al encriptar: {e} | Elemento: {element}")
            yield element


class DynamicSchemaFn(beam.DoFn):    
    def __init__(self, schema_manager):
        self.schema_manager = schema_manager
    
    def start_bundle(self):
        if self.schema_manager._schema_cache is None:
            bq_schema = self.schema_manager.load_schema()
            logger.info(
                f"DynamicSchemaFn inicializado con esquema de "
                f"{len(bq_schema)} campos de BigQuery"
            )
    
    def process(self, element: typing.Dict[str, typing.Any]) -> typing.Iterable[typing.Dict]:
        known_fields = set(self.schema_manager.get_all_fields())
        
        if not known_fields:
            logger.warning("No se pudo obtener el esquema de BigQuery. Pasando crudo.")
            yield element
            return
        
        filtered = {}
        extra_fields = []
        
        for k, v in element.items():
            if k in known_fields:
                filtered[k] = v
            else:
                extra_fields.append(k)
        
        if extra_fields:
            logger.warning(f" {len(extra_fields)} campos extra omitidos (no existen en BQ): {extra_fields}")
            
        yield filtered


class PrintFn(beam.DoFn):
    def process(self, element):
        if not element:
            return
        try:
            pretty = json.dumps(element, indent=2, ensure_ascii=False)
            logger.info(f"\nJSON final a insertar:\n{pretty}\n")
        except Exception as e:
            logger.warning(f"Error imprimiendo JSON: {e} | Elemento={element}")
        yield element

#PIPELINE PRINCIPAL
class SessionPipelineService:
    def __init__(self, options, cipher_adapter=None, cipher_service=None,
                 charset="utf-8", fields_to_encrypt=None):
        self.options = options
        self.cipher_adapter = cipher_adapter
        self.cipher_service = cipher_service
        self.charset = charset
        self.fields_to_encrypt = fields_to_encrypt or []
        
        self.schema_manager = SchemaManager(
            project_id=settings.projectData,
            dataset_id=settings.dataset,
            table_id=settings.table
        )
        
        logger.info("Cargando esquema inicial de BigQuery...")
        bq_schema = self.schema_manager.load_schema()
        
        if bq_schema:
            logger.info(f"Esquema cargado: {len(bq_schema)} campos")
            # logger.info(f"Campos: {sorted(bq_schema.keys())}") # Deshabilitado para no llenar de logs
        else:
            logger.warning("No se pudo cargar el esquema. Se procesará sin validación.")

    def run(self):
        session_secs = settings.sessionWindow
        lateness_secs = min(session_secs // 2, 120)

        session_minutes = session_secs // 60
        label_session_window = f"Session window ({session_minutes} minutes)"

        logger.info("Iniciando pipeline de sesiones")

        p = beam.Pipeline(options=self.options)
            
        encrypted_events = (
            p
            | "Read_PubSub_Data" >> PubSubAdapter(subscription=settings.pubsubData)
            | "Extract_and_Parse_Key" >> beam.ParDo(ExtractKeyValueFn())
            | label_session_window >> beam.WindowInto(window.Sessions(session_secs),allowed_lateness=lateness_secs,accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
            | "Merge_Events_Per_Key" >> beam.CombinePerKey(MergeEventsFn())
            | "Clean_Merged_Record" >> CleanMerged()
            | "Encrypt_Sensitive_Fields" >> beam.ParDo(EncryptFieldsFn(self.fields_to_encrypt,self.cipher_adapter,self.cipher_service,self.charset))
        )
        
        printed_encrypted_events = (
            encrypted_events
            | "Print_After_Encrypt" >> beam.ParDo(PrintFn())
        )

        cleaned_for_bq = (
            printed_encrypted_events 
            | "Clean_Data_Types" >> beam.ParDo(CleanQuoteDoFn(self.schema_manager))
            | "Validate_Dynamic_Schema" >> beam.ParDo(DynamicSchemaFn(self.schema_manager))
        )

        final_for_bq = cleaned_for_bq
        
        # Escritura a BigQuery
        _ = (
            final_for_bq
            | "Write_to_BigQuery" >> WriteToBigQuery(
                table=f"{settings.projectData}:{settings.dataset}.{settings.table}",
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                method="STREAMING_INSERTS",
                schema=self.schema_manager.get_bq_schema_for_beam()
            )
        )

        logger.info("Enviando pipeline a Dataflow...")

        return p.run()