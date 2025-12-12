# Aliases for Fink

# Source it from your .bashrc or .bash_aliases with:
# if [ -f $HOME/src/fink-broker/examples/alias-fink.sh ]; then
#     . $HOME/src/fink-broker/examples/alias-fink.sh
# fi

MYSHELL=$(echo $SHELL)
MYSHELL=$(basename $MYSHELL)
source <(finkctl completion $MYSHELL)

ASTROLAB_DIR="$HOME/src/github.com/astrolabsoftware"

FINK_BROKER_SRC_DIR="$ASTROLAB_DIR/fink-broker"
FINK_BROKER_IMAGES_SRC_DIR="$ASTROLAB_DIR/fink-broker-images"
FINK_ALERT_SIM_SRC_DIR="$ASTROLAB_DIR/fink-alert-simulator"
FINKCTL_SRC_DIR="$ASTROLAB_DIR/finkctl"

export FINKCONFIG=$FINK_BROKER_SRC_DIR/e2e/finkconfig_noscience

alias cdfa="cd $FINK_ALERT_SIM_SRC_DIR"
alias cdfb="cd $FINK_BROKER_SRC_DIR"
alias cdfbi="cd $FINK_BROKER_IMAGES_SRC_DIR"
alias cdfc="cd $FINKCTL_SRC_DIR"

alias fns="export NOSCIENCE=true"
alias fbp="$FINK_BROKER_SRC_DIR/build.sh && $FINK_BROKER_SRC_DIR/push-image.sh"

alias fadel="kubectl delete pod -l workflows.argoproj.io/completed"
alias fabp="$FINK_ALERT_SIM_SRC_DIR/build.sh && $FINK_ALERT_SIM_SRC_DIR/push-image.sh"
