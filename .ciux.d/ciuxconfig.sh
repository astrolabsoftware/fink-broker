# Source the ciux configuration file for the current project and selector.
# This script is intended to be run in a bash environment.

[ -n "$BASH_VERSION" ] || {
  echo "This script requires bash to run." >&2
  exit 1
}

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir=$dir/..

if [ -z "$SELECTOR" ]; then
  echo "Error: Selector environment variable (\$SELECTOR) is required." >&2
  exit 1
else
  echo "Using selector: $SELECTOR"
  selector_opt="--selector $SELECTOR"
fi

if ciuxconfig=$(ciux get configpath $selector_opt "$project_dir" 2>&1); then
  source "$ciuxconfig"
else
  echo "Error while loading ciux config : $ciuxconfig" >&2
  exit 1
fi
