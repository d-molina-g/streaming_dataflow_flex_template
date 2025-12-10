#!/bin/bash

# Si hay argumentos (Dataflow enviando flags de worker), ejecutamos el boot de Beam
if [ $# -ne 0 ]; then
    exec /opt/apache/beam/boot "$@"
fi

# Si NO hay argumentos (Flex Template iniciando), ejecutamos el launcher
exec /opt/google/dataflow/python_template_launcher