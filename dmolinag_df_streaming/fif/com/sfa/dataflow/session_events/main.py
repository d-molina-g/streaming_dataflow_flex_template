import logging
import json
import uuid

from google.cloud import secretmanager_v1beta1 as sm
from apache_beam.options.pipeline_options import PipelineOptions, WorkerOptions, GoogleCloudOptions
from fif.com.sfa.dataflow.session_events.application.session_pipeline import SessionPipelineService
from fif.com.sfa.dataflow.session_events.infrastructure.adapters.cypto_adapter import AESCipherAdapter
from fif.com.sfa.dataflow.session_events.infrastructure.service.cipher_service import CipherService
from fif.com.sfa.dataflow.session_events.config import settings


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def parse_labels(label_string: str) -> dict:
    if not label_string:
        return {}
    labels = {}
    for pair in label_string.split(","):
        if "=" in pair:
            k, v = pair.split("=", 1)
            labels[k.strip()] = v.strip()
    return labels


def access_secret_field(project_id: str, secret_id: str, field: str, version_id: str = "latest") -> str:
    try:
        client = sm.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})

        secret_data = response.payload.data.decode("UTF-8").strip()
        try:
            secret_json = json.loads(secret_data)
            if isinstance(secret_json, dict):
                if field not in secret_json:
                    raise KeyError(f"El campo '{field}' no existe en el secreto '{secret_id}'.")
                return secret_json[field]
            return secret_data

        except json.JSONDecodeError:
            return secret_data

    except Exception as e:
        logging.error(f"Error accediendo al campo '{field}' del secreto '{secret_id}': {e}")
        raise


def configure_pipeline_options() -> PipelineOptions:
    
    unique_suffix = uuid.uuid4().hex[:6]
    final_job_name = f"{settings.jobName}-{unique_suffix}"    
    logging.info(f"Generated Unique Job Name: {final_job_name}")

    options = PipelineOptions(
        flags=[],
        runner=settings.runner,
        streaming=settings.streaming,
        project=settings.projectWorker,
        region=settings.region,
        job_name=final_job_name,
        temp_location=settings.tempLocation,
        save_main_session=True,
    )

    # Convertir settings.labels en dict
    labels_dict = parse_labels(settings.labels)

    worker_options = options.view_as(WorkerOptions)
    worker_options.num_workers = settings.numWorkers
    worker_options.machine_type = settings.machineType
    worker_options.sdk_container_image = settings.sdkContainerImage





    if settings.runner.lower() == "dataflowrunner":
        worker_options.max_num_workers = settings.maxWorkers
        worker_options.autoscaling_algorithm = settings.autoscalingAlgorithm

        #worker_options.use_public_ips = settings.usePublicIps
        #worker_options.network = settings.network
        #worker_options.subnetwork = "regions/us-central1/subnetworks/df-streaming-subnet"


        gcloud_options = options.view_as(GoogleCloudOptions)
        gcloud_options.service_account_email = settings.serviceAccount
        gcloud_options.staging_location = settings.stagingLocation
        gcloud_options.labels = labels_dict
        

    return options


def run():
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Iniciando pipeline..")

    try:
        key = access_secret_field(settings.secretProject, settings.secretKey,"key")
        iv = access_secret_field(settings.secretProject, settings.secretKey,"iv")
        charset = settings.charsetEncode

        options = configure_pipeline_options()
        
        # Loguear las configuraciones de red para verificación
        worker_opts = options.view_as(WorkerOptions)
        logger.info(
            f"Runner: {settings.runner} | Proyecto: {settings.projectWorker} | "
            f"Network: {worker_opts.network or 'DEFAULT'} | Subnetwork: {worker_opts.subnetwork or 'N/A'}"
        )

        cipher_adapter = AESCipherAdapter(key, iv, charset)
        cipher_service = CipherService(cipher_adapter)
        fields_to_encrypt = settings.crypt_fields.split("|")


        logger.info("Construyendo pipeline...")
        pipeline_service = SessionPipelineService(
            options,
            cipher_adapter,
            cipher_service,
            charset,
            fields_to_encrypt
        )

        logger.info("Ejecutando pipeline...")
        pipeline_service.run()

        logger.info("Pipeline lanzado correctamente")

    except KeyError as e:
        logger.error(f"Error de configuración: {e}")
        raise

    except Exception as e:
        logger.error(f"Error crítico durante la ejecución: {e}")
        logger.exception("Stacktrace:")
        raise


if __name__ == "__main__":
    run()