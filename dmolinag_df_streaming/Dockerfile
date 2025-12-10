# 1. Usamos la imagen OFICIAL de Beam SDK como base (garantiza que funcione como Worker)
FROM apache/beam_python3.11_sdk:2.69.0

# 2. Copiamos el binario "launcher" desde la imagen de templates (para que funcione como Template)
COPY --from=gcr.io/dataflow-templates-base/python311-template-launcher-base:latest /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# 3. Variables de entorno básicas
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=fif/com/sfa/dataflow/session_events/main.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=setup.py
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /home/dataflow

# 4. Copiar dependencias y código (igual que antes)
COPY requirements.txt .
COPY setup.py .
COPY fif /home/dataflow/fif

# 5. Instalar dependencias del sistema (si es necesario)
RUN pip install .

# 6. Copiar el script de entrypoint personalizado
COPY entrypoint.sh /opt/google/dataflow/entrypoint.sh
RUN chmod +x /opt/google/dataflow/entrypoint.sh

# 7. Configuración Flex Template (Variables de entorno)
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=fif/com/sfa/dataflow/session_events/main.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=setup.py

# 8. Entrypoint. Usamos el script para decidir qué ejecutar
ENTRYPOINT ["/opt/google/dataflow/entrypoint.sh"]

# Limpiamos el CMD para que no interfiera
CMD []