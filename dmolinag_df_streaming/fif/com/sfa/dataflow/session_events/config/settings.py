import os
import sys


print(">>> INICIANDO CARGA DE SETTINGS (ARGV -> ENV) <<<")
for arg in sys.argv:
    if arg.startswith("--") and "=" in arg:
        # Quitamos el '--' inicial y separamos por el primer '='
        clean_arg = arg[2:]
        if "=" in clean_arg:
            key, val = clean_arg.split("=", 1)
            # Inyectamos en las variables de entorno
            os.environ[key] = val
            # Debug (Opcional: comenta esto en prod si hay datos sensibles)
            # print(f"   [Mapeado] {key} = {val}")
print(">>> FIN DE MAPEO <<<")



def get_env(*keys, default=None, cast=str):

    for key in keys:
        value = os.getenv(key)
        if value is not None:
            try:
                return cast(value)
            except ValueError:
                print(f"Error al convertir {key}='{value}' a {cast.__name__}")
                return default
    return default

def get_bool(*keys, default=False):
    return get_env(
        *keys,
        default=default,
        cast=lambda v: v.lower() in ("true", "1", "t", "yes")
    )

runner              = get_env("df_runner", default="DataflowRunner")
streaming           = get_bool("streaming")
projectWorker       = get_env("project_worker")
projectData         = get_env("project_data")
region              = get_env("df_region", default="us-central1")
jobName             = get_env("df_job_name")

# =============================================================================
# BUCKETS
# =============================================================================

tempLocation        = get_env("df_temp_location")
stagingLocation     = get_env("df_staging_location")

# =============================================================================
# WORKERS, LABELS, RED Y CUENTA DE SERVICIO
# =============================================================================

numWorkers          = get_env("df_num_workers", cast=int, default=1)
minWorkers          = get_env("df_min_num_workers", cast=int, default=1)
maxWorkers          = get_env("df_max_num_workers", cast=int, default=10)
machineType         = get_env("df_machine_type", default="n1-standard-1")
sdkContainerImage   = get_env("df_sdk_container_image", default="us-central1-docker.pkg.dev/david-molina-test/dataflow-templates/session-events-streaming:latest")

autoscalingAlgorithm = get_env("autoscaling_algorithm", default="THROUGHPUT_BASED")
network             = get_env("df_network", default="default")
usePublicIps        = get_bool("df_use_public_ips")
serviceAccount      = get_env("df_service_account")
labels              = get_env("labels")

# =============================================================================
# BIGQUERY Y PUB/SUB
# =============================================================================

dataset             = get_env("out_dataset")
table               = get_env("out_table", default="sfa-session_events")
pubsubData          = get_env("in_pubsub_sub")
pubsubDataConfig    = get_env("in_pubsub_config_sub")

# =============================================================================
# SECRET MANAGER
# =============================================================================

secretProject       = get_env("in_secret_project")
secretKey           = get_env("in_secret_key")
secretIv            = get_env("in_secret_iv")

# =============================================================================
# CRYPTO
# =============================================================================

charsetEncode       = get_env("in_crypt_charset")
crypt_fields       = get_env("in_crypt_fields")
fieldsEncrypt       = crypt_fields

# =============================================================================
# PARÁMETROS FUNCIONALES
# =============================================================================

eventKeyField       = get_env("in_event_key_field", default="quoteId")
sessionWindow       = get_env("in_session_window", cast=int, default=300)


#VariablesLocales

#runner="DataflowRunner"
#region="us-central1"
#machineType="n1-standard-1"  #n1-standard-2 carga media o n2-standard-2 // carga pesada n2-standard-4 o n2-highmem-4
#serviceAccount="inhapi-co-df-worker-dev@fif-sfa-cross-development.iam.gserviceaccount.com"
#numWorkers=2
#minWorkers=1
#maxWorkers=10
#jobName="sfa-co-inhapi-df-streaming-sessions-events-5"
#stagingLocation="gs://lnd_sfa_co_inhub_dev/quotations/staging"
#tempLocation="gs://lnd_sfa_co_inhub_dev/quotations/temp"

#projectWorker="fif-sfa-cross-development"
#projectData="fif-sfa-cross-development"
#pubsubData="projects/fif-sfa-cross-development/subscriptions/inhapi_quotation_ps_topic_dev_streaming_sub"
#eventKeyField="quoteId"
#sessionWindow=300
#secretProject="fif-sfa-cross-development"
#secretKey="sm_sfa_co_qa_vault_co_df_secret_buz"
#secretIv="sm_sfa_co_qa_vault_co_df_tweak"
#charsetEncode="utf-8"
#fieldsEncrypt="names,documentNumber,email,parternalLastName,maternalLastName,cellPhone"
#dataset="trf_master_inhub_dev"
#table="quotation_api"
#streaming=True
#labels="bu=sfa,compid=inhapi,country=co,dataflow_job=sfa-co-inhapi-df-streaming-sessions-events,scope=broker,type=trf,engine=web,pii=true"

##VPC##
#network = "dataflow-test-vpc-co"
#usePublicIps=False

print(" Dataflow Settings Loaded:")
print(f"  • Runner ................: {runner}")
print(f"  • Streaming .............: {streaming}")
print(f"  • Project Worker ........: {projectWorker}")
print(f"  • Region ................: {region}")
print(f"  • Job Name ..............: {jobName}")
print(f"  • Num Workers ...........: {numWorkers}")
print(f"  • Min Workers ...........: {minWorkers}")
print(f"  • Max Workers ...........: {maxWorkers}")
print(f"  • Machine Type ..........: {machineType}")
print(f"  • Network ...............: {network}")
print(f"  • Use Public IPs ........: {usePublicIps}")
print(f"  • Dataset ...............: {dataset}")
print(f"  • Table .................: {table}")
print(f"  • Session Window (s) ....: {sessionWindow}")
print(f"  • Key Field .............: {eventKeyField}")
print(f"  • Pub/Sub Subscription ..: {pubsubData}")
print(f"  • Service Account .......: {serviceAccount}")
print(f"  • secret Project .......:  {secretProject}")
print(f"  • secret Key .......:      {secretKey}")
print(f"  • secretIv .......:        {secretIv}")
print(f"  • fields key .......:      {crypt_fields}")
print(" Settings ready.\n")