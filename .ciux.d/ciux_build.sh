# Label selector: build
export FINK_BROKER_DIR=/home/fjammes/src/github.com/astrolabsoftware/fink-broker
export FINK_BROKER_VERSION=v3.2.0-rc0-81-g0a1a786-dirty
export FINK_BROKER_WORKBRANCH=1096-add-fink-broker-dockerfile-for-k8s
export ASTROLABSOFTWARE_FINK_FINK_DEPS_NOSCIENCE_ZTF_IMAGE=gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-deps-noscience-ztf:v2.52.0-69-g662d30f
export ASTROLABSOFTWARE_FINK_FINK_DEPS_SCIENCE_ZTF_IMAGE=gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-deps-science-ztf:v2.52.0-69-g662d30f
export CIUX_IMAGE_REGISTRY=gitlab-registry.in2p3.fr/astrolabsoftware/fink
export CIUX_IMAGE_NAME=fink-broker-noscience
# Image which contains latest code source changes FINK_BROKER_VERSION
export CIUX_IMAGE_TAG=v3.2.0-rc0-81-g0a1a786
export CIUX_IMAGE_URL=gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-broker-noscience:v3.2.0-rc0-81-g0a1a786
# True if CIUX_IMAGE_URL need to be built
export CIUX_BUILD=false
# Promoted image is the image which will be push if CI run successfully
export CIUX_PROMOTED_IMAGE_URL=gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-broker-noscience:v3.2.0-rc0-81-g0a1a786-dirty