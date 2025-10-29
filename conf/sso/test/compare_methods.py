"""Simply compare local installation of eproc with web service results"""
from fink_utils.sso.miriade import query_miriade_ephemcc
from fink_utils.sso.miriade import query_miriade
from astropy.coordinates import SkyCoord
import astropy.units as u
import time

import requests
import pandas as pd
import numpy as np
import io

#ident = "10P"
#ident = "2010 JO69"
#ident = "C/2020_R2"
#ident = "302530"
#ident = "143"
#ident = "Julienpeloton"
ident = "9104_P-L"

r = requests.post(
    'https://api.ztf.fink-portal.org/api/v1/sso',
    json={
        'n_or_d': ident,
        'output-format': 'json'
    }
)

# Format output in a DataFrame
pdf = pd.read_json(io.BytesIO(r.content))

jd = pdf["i:jd"].to_numpy()

parameters = {
    'outdir': '/tmp/ramdisk/spins',
    'runner_path': '/tmp/fink_run_ephemcc4.4.sh',
    'userconf': '/tmp/.eproc-4.4',
    'iofile': '/tmp/default-ephemcc-observation.xml'
}

t0 = time.time()
ephem_1 = query_miriade_ephemcc(
    ident,
    jd,
    parameters=parameters
)
print("LOCAL: {:.2f} seconds".format(time.time() - t0))

t0 = time.time()
ephem_2 = query_miriade(ident, jd)
print("REMOTE: {:.2f} seconds".format(time.time() - t0))

common_cols = [col for col in ephem_1.columns if col in ephem_2.columns]
for col in common_cols:
    a = ephem_1[col].to_numpy()
    b = ephem_2[col].to_numpy()
    if isinstance(a[0], int) or isinstance(a[0], float):
        print(
            "{}: median deviation = {:.2f}% (max = {:.2f}%)".format(
                col,
                np.median(np.abs(1 - a/b)) * 100,
                np.max(np.abs(1 - a/b)) * 100
            )
        )
    else:
        print("Skip {}".format(col))

# RA/DEC
coord_1 = SkyCoord(ephem_1["RA"], ephem_1["DEC"], unit=(u.deg, u.deg))
coord_2 = SkyCoord(ephem_2["RA"], ephem_2["DEC"], unit=(u.deg, u.deg))
dev = coord_1.separation(coord_2).deg * 3600
print("Median position deviation: {:.2f} arcsec (max = {:.2f} arcsec)".format(np.median(dev), np.max(dev)))

