# Set variable below to empty string in order to
# build and install current development version
FINK_BROKER_RELEASE=''

# Do not launch science pipeline if true
NOSCIENCE="${NOSCIENCE:-false}"

# Set minimal limits/requests for Spark driver and executor if true
MINIMAL="${MINIMAL:-false}"


# Build parameters
# ----------------
# Repository address
REPO="gitlab-registry.in2p3.fr/astrolabsoftware/fink"
CI_REPO="${CI_REPO:-$REPO}"
# Tag to apply to the built image, or to identify the image to be pushed
TAG=${FINK_BROKER_RELEASE:-$(git -C $DIR describe --dirty --always)}
# WARNING "spark-py" is hard-coded in spark build script

# Disable science pipeline
if [ "$NOSCIENCE" = true ];
then
  IMAGE="$CI_REPO/fink-broker-noscience:$TAG"
  PROMOTED_IMAGE="$REPO/fink-broker-noscience:$TAG"
else
  IMAGE="$CI_REPO/fink-broker:$TAG"
  PROMOTED_IMAGE="$REPO/fink-broker:$TAG"
fi

# Spark parameters
# ----------------
# Assuming Scala 2.11

# Spark version
SPARK_VERSION="3.4.1"

# Spark image tag
# Spark image is built here: https://github.com/astrolabsoftware/k8s-spark-py/
SPARK_IMAGE_TAG="k8s-$SPARK_VERSION"


# Name for the Spark archive
SPARK_NAME="spark-${SPARK_VERSION}-bin-hadoop3"

# Spark install location
SPARK_INSTALL_DIR="${HOME}/fink-k8s-tmp"

export SPARK_HOME="${SPARK_INSTALL_DIR}/${SPARK_NAME}"
export PATH="$SPARK_HOME/bin:$PATH"

# Kafka cluster parameters
# ------------------------
# Name for Kafka cluster
KAFKA_NS="kafka"
KAFKA_CLUSTER="kafka-cluster"


# Spark job 'stream2raw' parameters
# ---------------------------------
# Default values are the ones set in fink-alert-simulator CI environment
KAFKA_SOCKET=${KAFKA_SOCKET:-"kafka-cluster-kafka-external-bootstrap.kafka:9094"}
KAFKA_TOPIC=${KAFKA_TOPIC:-"ztf-stream-sim"}

FINK_ALERT_SIMULATOR_DIR="/tmp/fink-alert-simulator"
