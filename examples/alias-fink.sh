# Aliases for Fink

# Source it from your .bashrc or .bash_aliases with:
# if [ -f $HOME/src/fink-broker/examples/alias-fink.sh ]; then
#     . $HOME/src/fink-broker/examples/alias-fink.sh
# fi


FINK_BROKER_SRC_DIR="$HOME/src/fink-broker"
FINK_ALERT_SIM_SRC_DIR="$HOME/src/fink-alert-simulator"
FINKCTL_SRC_DIR="$HOME/src/finkctl"

alias cdfa="cd $FINK_ALERT_SIM_SRC_DIR"
alias cdfb="cd $FINK_BROKER_SRC_DIR"
alias cdfc="cd $FINKCTL_SRC_DIR"

alias fns="export MINIMAL=true NOSCIENCE=true"
alias fbp="$FINK_BROKER_SRC_DIR/build.sh && $FINK_BROKER_SRC_DIR/push-image.sh"

alias fadel="kubectl delete pod -l workflows.argoproj.io/completed"
alias fabp="$FINK_ALERT_SIM_SRC_DIR/build.sh && $FINK_ALERT_SIM_SRC_DIR/push-image.sh"