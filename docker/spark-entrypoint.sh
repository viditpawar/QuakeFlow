#!/bin/sh
# Render the Spark/Iceberg catalog config from its template, then run the requested command.
# Every ${VAR} in the template must be set, otherwise start-up fails instead of using a blank.
set -eu

CONF_DIR=/tmp/spark-conf
mkdir -p "$CONF_DIR"
python3 - <<'PY'
import os
import string

template = open("/opt/quakeflow/conf/spark-defaults.conf.template", encoding="utf-8").read()
rendered = string.Template(template).substitute(os.environ)  # KeyError names a missing variable
open("/tmp/spark-conf/spark-defaults.conf", "w", encoding="utf-8").write(rendered)
PY
export SPARK_CONF_DIR="$CONF_DIR"

exec /opt/entrypoint.sh "$@"
