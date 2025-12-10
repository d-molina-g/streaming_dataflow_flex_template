import logging
from typing import Dict, Optional, List
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class SchemaManager:    
    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self._schema_cache: Optional[Dict[str, str]] = None 
        self._bq_schema_fields: Optional[List[bigquery.SchemaField]] = None 
        
        logger.info(
            f"SchemaManager configurado para: "
            f"{project_id}.{dataset_id}.{table_id}"
        )
    
    def _get_client(self) -> bigquery.Client:
        return bigquery.Client(project=self.project_id)
    
    def load_schema(self, force_reload: bool = False) -> Dict[str, str]:
        if self._schema_cache is not None and not force_reload:
            logger.debug("Usando esquema en caché")
            return self._schema_cache
        
        try:
            client = self._get_client()
            table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
            table = client.get_table(table_ref)
            
            schema_dict = {}
            self._bq_schema_fields = table.schema
            
            for field in table.schema:
                schema_dict[field.name] = field.field_type
            
            self._schema_cache = schema_dict
            
            logger.info(
                f"Esquema de BigQuery cargado correctamente: "
                f"{len(schema_dict)} campos detectados"
            )
            logger.debug(f"Campos del esquema: {list(schema_dict.keys())}")
            
            return schema_dict
            
        except Exception as e:
            logger.error(
                f"Error al cargar el esquema de BigQuery: {e}\n"
                f"Tabla: {self.project_id}.{self.dataset_id}.{self.table_id}"
            )
            self._bq_schema_fields = None
            return {}
    
    # --- MÉTODO PARA BEAM ---
    def get_bq_schema_for_beam(self) -> Optional[Dict]:
        if self._bq_schema_fields is None:
            self.load_schema() 
        
        if not self._bq_schema_fields:
            return None

        field_list = [field.to_api_repr() for field in self._bq_schema_fields]

        return {"fields": field_list}
    
    # --- MÉTODOS EXISTENTES 
    def get_schema(self) -> Dict[str, str]:
        if self._schema_cache is None:
            return self.load_schema()
        return self._schema_cache
    
    def reload_schema(self) -> Dict[str, str]:
        logger.info("Recargando esquema de BigQuery...")
        return self.load_schema(force_reload=True)
    
    def field_exists(self, field_name: str) -> bool:
        schema = self.get_schema()
        return field_name in schema
    
    def get_field_type(self, field_name: str) -> Optional[str]:
        schema = self.get_schema()
        return schema.get(field_name)
    
    def get_all_fields(self) -> list:
        schema = self.get_schema()
        return list(schema.keys())