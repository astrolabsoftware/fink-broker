# How to Build a Fink Release

This guide provides step-by-step instructions on how to cut a new release for the `fink-broker`.

## Get Source Repositories for the Release

1. **Check the `fink-broker` CI**

Check the `fink-broker` CI for `main` branch to check the new release pass successfully integration (e2e) tests.

Url for the CI is: https://github.com/astrolabsoftware/fink-broker/actions

2. **Clone the Fink-broker source code and retrieve dependencies**:

    ```bash
    git clone https://github.com/astrolabsoftware/fink-broker.git
    ciux get deps ./fink-broker -l release
    ```

    Clone all the necessary repositories and ensure you are using their `master/main` branch.

## Get Release Tag

1. **Generate a new release tag in the `fink-broker` directory**:

    ```bash
    ciux tag .
    ```

    This command will return a new tag. Update it if needed.

    ```bash
    RELEASE_TAG="vX.Y.Z-rcT"
    ```

2. **Apply the release tag to all `fink-broker` dependency repositories**:

    ```bash
    git pull && git tag -m "Release $RELEASE_TAG" $RELEASE_TAG && git push --tags
    ```

3. **Wait for the `fink-alert-simulator` CI to succeed**:

    Ensure the CI for `fink-alert-simulator` is successful to obtain the `fink-alert-simulator` container image for the release.

    Check locally the image is avaible:

    ```bash
    docker pull gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-alert-simulator:"$RELEASE_TAG"
    ```

4. **Apply the release tag to the `fink-broker` source code**:

    ```bash
    git pull && git tag -m "Release $RELEASE_TAG" $RELEASE_TAG && git push --tags
    ```

## Final Steps

Check the `fink-broker` CI to confirm that the integration tests are running successfully. Once confirmed, the release is ready.