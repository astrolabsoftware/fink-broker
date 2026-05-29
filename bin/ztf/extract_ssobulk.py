# Copyright 2019-2026 AstroLab Software
# Author: Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Extract all SSO lightcurves with residuals from the sHG1G2 model."""

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, FloatType

import pandas as pd
import numpy as np
import datetime
import argparse
from line_profiler import profile

from fink_utils.sso.spins import func_shg1g2

import logging


_LOG = logging.getLogger(__name__)
SSO_FILE = "sso_ztf_lc_aggregated_w_ssoft_{}{}.parquet"


@pandas_udf(ArrayType(FloatType()), PandasUDFType.SCALAR)
@profile
def extract_residuals(
    fid,
    magpsf,
    raobs,
    decobs,
    phase,
    dobs,
    dhelio,
    H_1,
    H_2,
    G1_1,
    G1_2,
    G2_1,
    G2_2,
    R,
    alpha0,
    delta0,
    fit,
):
    """Apply the sHG1G2 model to the data

    Returns
    -------
    out: pd.Series of list
        Magnitude residuals
    """
    pdf = pd.DataFrame({
        "residuals": [[] for i in range(len(raobs))],
        "fid": fid.to_numpy(),
        "ra": raobs.to_numpy(),
        "dec": decobs.to_numpy(),
        "phase": phase.to_numpy(),
        "magpsf": magpsf.to_numpy(),
        "dobs": dobs.to_numpy(),
        "dhelio": dhelio.to_numpy(),
        "R": R.to_numpy(),
        "alpha0": alpha0.to_numpy(),
        "delta0": delta0.to_numpy(),
        "H_1": H_1.to_numpy(),
        "H_2": H_2.to_numpy(),
        "G1_1": G1_1.to_numpy(),
        "G1_2": G1_2.to_numpy(),
        "G2_1": G2_1.to_numpy(),
        "G2_2": G2_2.to_numpy(),
        "fit": fit.to_numpy(),
    })
    for index, row in pdf.iterrows():
        if row["fit"] != 0:
            # Fit failed
            pdf["residuals"].iloc[index] = []
            continue

        magpsf_red = row["magpsf"] - 5 * np.log10(row["dobs"] * row["dhelio"])
        container = np.zeros(len(magpsf_red))
        for filt in np.unique(row["fid"]):
            cond = row["fid"] == filt
            model = func_shg1g2(
                [
                    np.deg2rad(row["phase"][cond]),
                    np.deg2rad(row["ra"][cond]),
                    np.deg2rad(row["dec"][cond]),
                ],
                row["H_{}".format(filt)],
                row["G1_{}".format(filt)],
                row["G2_{}".format(filt)],
                row["R"],
                np.deg2rad(row["alpha0"]),
                np.deg2rad(row["delta0"]),
            )
            container[cond] = magpsf_red[cond] - model
        pdf["residuals"].iloc[index] = container
    return pdf["residuals"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "-limit",
        type=int,
        default=None,
        help="""
        Use only `limit` number of SSO for test purposes.
        Default is None, meaning all available data is considered.
        """,
    )
    args = parser.parse_args(None)

    # Get current year and month
    year = datetime.datetime.now().year
    month = "{:02}".format(datetime.datetime.now().month)

    spark = SparkSession.builder.appName(
        "sso_bulk_{}{}".format(year, month)
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Load SSO LC data
    df_lc = spark.read.format("parquet").load(
        "/user/julien.peloton/sso_ztf_lc_aggregated_{}{}.parquet".format(year, month)
    )

    # Load SSOFT data
    df_ssoft = spark.read.format("parquet").load(
        "/user/livy/SSOFT/ssoft_SHG1G2_{}.{}.parquet".format(year, month)
    )

    # Join
    df = df_lc.join(df_ssoft, on="ssnamenr").cache()
    nobjects = df.count()
    _LOG.info("{} objects in the SSOBULK".format(nobjects))

    if args.limit is not None:
        assert isinstance(args.limit, int), (args.limit, type(args.limit))
        _LOG.info("Limiting the new number of objects to {}".format(args.limit))
        df = df.limit(args.limit)

    df = df.withColumn(
        "residuals_shg1g2",
        extract_residuals(
            df["cfid"],
            df["cmagpsf"],
            df["cra"],
            df["cdec"],
            df["Phase"],
            df["Dobs"],
            df["Dhelio"],
            df["H_1"],
            df["H_2"],
            df["G1_1"],
            df["G1_2"],
            df["G2_1"],
            df["G2_2"],
            df["R"],
            df["alpha0"],
            df["delta0"],
            df["fit"],
        ),
    )

    df.write.mode("append").parquet(SSO_FILE.format(year, month))


if __name__ == "__main__":
    main()
