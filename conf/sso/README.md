# Installation of SSO tools

This folder contains scripts to install all tools for SSO in Fink.

## Installation of eproc & co

Full procedure can be found on the Fink GitLab:

1. Installing Miriade on the master node: [link](https://gitlab.in2p3.fr/fink/rubin-performance-check/-/tree/main/sso?ref_type=heads#installing-miriade-on-the-spark-cluster)
2. Installation on all executors: [link](https://gitlab.in2p3.fr/fink/rubin-performance-check/-/tree/main/sso?ref_type=heads#installation-on-all-executors)

## Testing the installation

Go to the `test` folder and execute:

```bash
./check_ephemcc.sh
```

This will check the `ephemcc` binary. Then execute:

```bash
python compare_methods.py
```

This will test local version of ephemcc against remote ephemcc (IMCCE web service). Residuals should be small!

```bash
# On object 9104_P-L
LOCAL: 3.36 seconds
REMOTE: 4.79 seconds
Date: median deviation = 0.00% (max = 0.00%)
HA: median deviation = 0.00% (max = 22.92%)
Az: median deviation = 0.00% (max = 0.15%)
H: median deviation = 0.00% (max = 0.17%)
Dobs: median deviation = 0.00% (max = 0.00%)
Dhelio: median deviation = 0.00% (max = 0.00%)
VMag: median deviation = 0.00% (max = 0.00%)
Phase: median deviation = 0.00% (max = 0.00%)
Elong.: median deviation = 0.00% (max = 0.00%)
AM: median deviation = 0.00% (max = 0.13%)
dRAcosDEC: median deviation = 0.00% (max = 0.03%)
dDEC: median deviation = 0.00% (max = 0.00%)
RV: median deviation = 0.00% (max = 0.01%)
Median deviation: 0.01 arcsec (max = 0.11 arcsec)
```
