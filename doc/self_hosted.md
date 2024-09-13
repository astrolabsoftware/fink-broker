# Set up fink self-hosted CI

Here's a documented procedure for creating a Linux user account on Ubuntu, adding it to the Docker group, cloning a Git repository inside a script (`run_script.sh`), and running that script nightly with a cron job, using a token stored in the user’s home directory.

## Pre-requisites

- a Linux server with sudo access
- Docker 24.0.2+
- git 2.17.1+
- go 1.22.5+
- A Github token with the "Content" permission on the `fink-broker` Github repository.

## Steps

### List of commands

```bash
# Create user
sudo adduser fink-ci --disabled-password

# Add to Docker group
sudo usermod -aG docker fink-ci

sudo su - fink-ci

# Store GitHub token securely
echo "your_github_token" > /home/fink-ci/.token
chmod 600 /home/fink-ci/.token

cat <<EOF > /home/fink-ci/fink-ci.sh
#!/bin/bash

set -euxo pipefail

# Load GitHub token
export TOKEN=$(cat /home/fink-ci/.token)
export USER="fink-ci"
repo_url="https://github.com/astrolabsoftware/fink-broker.git"
tmpdir=\$(mktemp -d --suffix -fink-broker-ci)
repo=\$tmpdir/fink-broker
branchname="877-automated-e2e-tests"

# Set go path according to go install method
PATH=\$HOME/go/bin:/usr/local/go/bin:/usr/local/bin:\$PATH

# Clone the repository
git clone --single-branch \$repo_url \$repo --branch \$branchname

# Run fink ci in science mode
\$repo/e2e/run.sh -s -c
EOF

# Make the script executable
chmod +x /home/fink-ci/fink-ci.sh
```

### Set Up the Cron Job to run nightly
To ensure the CI script runs nightly, set up a cron job.

1. Open the crontab for the `fink-ci` user:

   ```bash
   crontab -e
   ```

2. Add the following line to schedule the script to run at midnight every day:

   ```bash
   0 0 * * * /home/fink-ci/fink-ci.sh >> /home/fink-ci/cronjob-$(date +\%Y-\%m-\%d).log 2>&1
   ```

   This will log the output to `/home/fink-ci/cronjob-YYY-MM-DD.log`.


By following this procedure, the `fink-ci` user will be set up to automatically run fink e2e tests every night and report it to Github Actions.