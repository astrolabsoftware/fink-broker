# Spark parameters
# ----------------

# TODO manage spark inside container (or with ciux)?

# Spark version
SPARK_VERSION="3.4.1"

# Name for the Spark archive
SPARK_NAME="spark-${SPARK_VERSION}-bin-hadoop3"

# Spark install location
SPARK_INSTALL_DIR="${HOME}/fink-k8s-tmp"

export SPARK_HOME="${SPARK_INSTALL_DIR}/${SPARK_NAME}"
export PATH="$SPARK_HOME/bin:$PATH"
