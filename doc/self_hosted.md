# Set up fink self-hosted CI

Here's a documented procedure for creating a Linux user account on Ubuntu, adding it to the Docker group, cloning a Git repository inside a script (`run_script.sh`), and running that script nightly with a cron job, using a token stored in the user’s home directory.

## Pre-requisites

- a Linux server with sudo access
- Docker
- git
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

cat <<EOF >> /home/fink-ci/fink-ci.sh
#!/bin/bash

set -euxo pipefail

# Load GitHub token
TOKEN=$(cat /home/fink-ci/.token)

REPO_URL="https://github.com/astrolabsoftware/fink-broker.git"

REPO=/home/fink-ci/fink-broker

# Clone the repository or pull the latest changes
if [ ! -d "\$REPO" ]; then
    git clone \$REPO_URL \$REPO
else
    cd \$REPO
    git -C \$REPO pull
fi

# Run fink ci in science mode
\$REPO/e2e/run.sh -s
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