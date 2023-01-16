# Build parameters
# ----------------
# Repository address
readonly REPO="gitlab-registry.in2p3.fr/astrolabsoftware/fink"
# Tag to apply to the built image, or to identify the image to be pushed
GIT_HASH="$(git -C $DIR describe --dirty --always)"
readonly IMAGE_TAG="$GIT_HASH"
# WARNING "spark-py" is hard-coded in spark build script
readonly IMAGE="$REPO/fink-broker:$IMAGE_TAG"
