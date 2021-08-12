#!/usr/bin/env python
# Copyright 2021 AstroLab Software
# Author: Julien Peloton, Sergey Karpov
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
"""Extract tracklet ID from the alert data
"""
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType

import os
import numpy as np
import pandas as pd
from astropy.coordinates import SkyCoord

from fink_broker.tester import spark_unit_tests

def apply_tracklet_cuts(df: DataFrame) -> DataFrame:
    """ Select potential tracklet candidates based on property cuts.

    We first apply 3 criteria to select interesting candidates:

    1. remove alerts with possible counterpart in MPC
    2. remove alerts with possible counterpart in SIMBAD
    3. keep only alerts with 1 detection

    Then, based on Sergey's analysis, we limit the analysis to
    the candidates outside the locus of bad subtractions.

    Parameters
    ----------
    df: Spark DataFrame
        Input dataframe containing alert data

    Returns
    ----------
    df_filt: Spark DataFrame
        Spark DataFrame of smaller size containing only potential
        tracklet candidate data based on the cuts.

    Examples
    ----------
    >>> df = spark.read.format('parquet').load(ztf_alert_sample)
    >>> df_filt = apply_tracklet_cuts(df)
    >>> df_filt.count()
    16
    """
    # remove known asteroids
    f1 = df['candidate.ssnamenr'] == 'null'

    # Keep only unknown objects
    f2 = df['cdsxmatch'].isin(['Unknown'])

    # Keep only objects with 1 detection
    f3 = df['candidate.ndethist'] == 1

    hypot = F.sqrt(df['candidate.sigmagnr']**2 + df['candidate.sigmapsf']**2)
    shiftlog = F.log10(df['candidate.distnr']) + 0.2
    f4 = (df['candidate.magnr'] - df['candidate.magpsf']) < (3.0 * hypot)
    f5 = (df['candidate.magnr'] - df['candidate.magpsf']) < (-4 * shiftlog)
    f6 = df['candidate.distnr'] < 2

    df_filt = df.filter((f1 & f2 & f3) & ~(f4 & f5 & f6)).cache()

    return df_filt

def add_tracklet_information(df: DataFrame) -> DataFrame:
    """ Add a new column to the DataFrame with tracklet ID

    Parameters
    ----------
    df: Spark DataFrame
        Input Spark DataFrame containing alert data

    Returns
    ----------
    df_out: Spark DataFrame
        Spark DataFrame containing alert data, plus a column `tracklet`
        with tracklet ID (str)

    Examples
    ----------
    >>> df = spark.read.format('parquet').load(ztf_alert_sample)
    >>> df_tracklet = add_tracklet_information(df)
    >>> df_tracklet.select('tracklet').take(1)[0][0]
    'TRCK1615_00'
    """
    df_filt = apply_tracklet_cuts(df)

    df_filt_tracklet = df_filt.withColumn(
        'tracklet',
        F.lit('')
    ).select(
        [
            'candid',
            'candidate.nid',
            'tracklet',
            'candidate.ra',
            'candidate.dec'
        ]
    )

    @pandas_udf(df_filt_tracklet.schema, PandasUDFType.GROUPED_MAP)
    def extract_tracklet_number(pdf: pd.DataFrame) -> pd.DataFrame:
        """ Extract tracklet ID from a Spark DataFrame

        This pandas UDF must be used with grouped functions (GROUPED_MAP),
        as it processes night-by-night.

        Parameters
        ----------
        pdf: Pandas DataFrame
            Pandas DataFrame from a Spark groupBy. It needs to have at least
            4 columns: ra, dec, nid, and tracklet. The tracklet column is
            initially empty (string), and it is filled by this function.

        Returns
        ----------
        pdf: Pandas DataFrame
            The same Pandas DataFrame as the input one, but the column
            `tracklet` has been updated with tracklet ID information.
        """
        ra = pdf['ra']
        dec = pdf['dec']
        nid = pdf['nid']
        nid_ = nid.values[0]
        # String - container for tracklet designation
        tracklet_numbers = pdf['tracklet']

        # Coordinates of the objects
        coords = SkyCoord(ra.values, dec.values, unit='deg')
        xyz = coords.cartesian
        # unit vectors corresponding to the points, Nx3
        xyz = xyz.xyz.value.T

        if len(ra) < 5:
            return pdf

        # Levi-Civitta symbol
        eijk = np.zeros((3, 3, 3))
        eijk[0, 1, 2] = eijk[1, 2, 0] = eijk[2, 0, 1] = 1
        eijk[0, 2, 1] = eijk[2, 1, 0] = eijk[1, 0, 2] = -1

        # cross-products, NxNx3
        circles = np.einsum('ijk,uj,vk->uvi', eijk, xyz, xyz, optimize=True)
        # norms, i.e. arc sine lengths, NxN
        norms = np.sqrt(
            np.einsum('uvi,uvi->uv', circles, circles, optimize=True)
        )
        # Remove redundant entries corresponding to
        # the symmetry on point swapping
        norms = np.tril(norms)

        # Pairs with angular separation larger than 10 arcsec, NxN
        norm_idx = norms > 10 / 206265

        circles[norms == 0, :] = 0
        # normals to great circles, NxNx3
        circles[norms > 0, :] /= norms[norms > 0, np.newaxis]

        # Sets of points along great circles
        cidxs = []

        for i, point in enumerate(xyz):
            # sine distance from the great circle
            sindists = np.einsum('vi,ki->vk', circles[i], xyz, optimize=True)
            # Good distances from great circles, NxN
            sin_idx = np.abs(sindists) < 1 / 206265

            # Numbers of points along each great circle, N
            nps = np.einsum(
                'uv->u',
                (norm_idx[i, :, np.newaxis] & sin_idx).astype(np.int8),
                optimize=True
            )
            np_idx = nps >= 5

            cidxs += list(sin_idx[np_idx])

        if len(cidxs) == 0:
            return pdf

        uniq = np.unique(cidxs, axis=0)
        aidx = np.argsort([-np.sum(_) for _ in uniq])

        used = np.zeros(len(ra), dtype=bool)

        index_tracklet = 0
        for cidx in uniq[aidx]:
            # First we need to reject the tracklets
            # mostly superceded by longer ones
            if np.sum(cidx[~used]) < 5:
                # TODO: should we try to greedily merge the extra points
                # into already known tracks?
                continue

            tracklet_positions = cidx

            used[cidx] = True

            tracklet_numbers[tracklet_positions] = 'TRCK{}_{:02d}'.format(
                nid_,
                index_tracklet
            )
            index_tracklet += 1

        return pdf.assign(tracklet=tracklet_numbers)

    df_trck = df_filt_tracklet\
        .cache()\
        .groupBy('nid')\
        .apply(extract_tracklet_number)\
        .select(['candid', 'tracklet'])\
        .filter(F.col('tracklet') != '')\
        .dropDuplicates(['candid'])

    df_out = df\
        .join(
            df_trck.select(['candid', 'tracklet']),
            on='candid',
            how='outer'
        )

    return df_out


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ['FINK_HOME']
    globs["ztf_alert_sample"] = os.path.join(
        root, "ztf_alerts/tracklet_TRCK1615_00")

    # Run the Spark test suite
    spark_unit_tests(globs, withstreaming=True)
