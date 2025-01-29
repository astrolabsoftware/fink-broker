#!/usr/bin/env python
# Copyright 2021-2024 AstroLab Software
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
"""Extract tracklet ID from the alert data"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType

import os
import numpy as np
import pandas as pd
from astropy.coordinates import SkyCoord
from astropy.time import Time

from fink_broker.common.tester import spark_unit_tests


def apply_tracklet_cuts(df: DataFrame) -> DataFrame:
    """Select potential tracklet candidates based on property cuts.

    We first apply 3 criteria to select interesting candidates:

    1. remove alerts with possible counterpart in MPC
    2. remove alerts with negative fluxes
    3. keep only alerts with 1 detection

    Then, based on Sergey's analysis, we limit the analysis to
    the candidates outside the locus of variable stars (and bad subtractions).

    Parameters
    ----------
    df: Spark DataFrame
        Input dataframe containing alert data

    Returns
    -------
    df_filt: Spark DataFrame
        Spark DataFrame of smaller size containing only potential
        tracklet candidate data based on the cuts.

    Examples
    --------
    >>> df = spark.read.format('parquet').load(ztf_alert_sample)
    >>> df_filt = apply_tracklet_cuts(df)
    >>> df_filt.count()
    16
    """
    # remove known asteroids
    idx = df["candidate.ssnamenr"] == "null"

    # Keep only objects unknown to SIMBAD - seems unnecessary
    # idx &= df['cdsxmatch'].isin(['Unknown'])

    # Keep only objects with 1 detection
    idx &= df["candidate.ndethist"] == 1

    # Keep only positive detections
    idx &= df["candidate.isdiffpos"] == "t"

    # Simple definition of locus containing (most of) stellar variability
    # as well as bad subtraction - basically, the variations are fainter than
    # the template object itself, and distance is smaller than typical FWHM
    shiftlog = F.log10(df["candidate.distnr"]) + 0.2
    nidx = (df["candidate.magnr"] - df["candidate.magpsf"]) < 1.0
    nidx &= (df["candidate.magnr"] - df["candidate.magpsf"]) < (-4 * shiftlog)
    nidx &= df["candidate.distnr"] < 2

    df_filt = df.filter(idx & ~nidx).cache()

    return df_filt


def add_tracklet_information(df: DataFrame) -> DataFrame:
    """Add a new column to the DataFrame with tracklet ID

    Parameters
    ----------
    df: Spark DataFrame
        Input Spark DataFrame containing alert data

    Returns
    -------
    df_out: Spark DataFrame
        Spark DataFrame containing alert data, plus a column `tracklet`
        with tracklet ID (str)

    Examples
    --------
    >>> df = spark.read.format('parquet').load(ztf_alert_sample)
    >>> df_tracklet = add_tracklet_information(df)
    >>> df_tracklet.select('tracklet').take(2)[1][0]
    'TRCK_20210604_093609_00'
    """
    # Select potential candidates
    df_filt = apply_tracklet_cuts(df)

    # Initialise `tracklet` column
    df_filt_tracklet = df_filt.withColumn("tracklet", F.lit("")).select([
        "candid",
        "candidate.jd",
        "candidate.xpos",
        "candidate.ypos",
        "candidate.nid",
        "tracklet",
        "candidate.ra",
        "candidate.dec",
    ])

    @pandas_udf(df_filt_tracklet.schema, PandasUDFType.GROUPED_MAP)
    def extract_tracklet_number(pdf: pd.DataFrame) -> pd.DataFrame:
        """Extract tracklet ID from a Spark DataFrame

        This pandas UDF must be used with grouped functions (GROUPED_MAP),
        as it processes exposure-by-exposure.

        Parameters
        ----------
        pdf: Pandas DataFrame
            Pandas DataFrame from a Spark groupBy. It needs to have at least
            4 columns: ra, dec, jd, and tracklet. The tracklet column is
            initially empty (string), and it is filled by this function.

        Returns
        -------
        pdf: Pandas DataFrame
            The same Pandas DataFrame as the input one, but the column
            `tracklet` has been updated with tracklet ID information.
        """
        ra = pdf["ra"]
        dec = pdf["dec"]
        jd = pdf["jd"]
        time_str = Time(jd.to_numpy()[0], format="jd").strftime("%Y%m%d_%H%M%S")
        # String - container for tracklet designation
        tracklet_names = pdf["tracklet"]

        # Coordinates of the objects
        coords = SkyCoord(ra.to_numpy(), dec.to_numpy(), unit="deg")
        xyz = coords.cartesian
        # unit vectors corresponding to the points, Nx3
        xyz = xyz.xyz.value.T

        if len(ra) < 5:
            return pdf

        # Levi-Civitta symbol
        eijk = np.zeros((3, 3, 3))
        eijk[0, 1, 2] = eijk[1, 2, 0] = eijk[2, 0, 1] = 1
        eijk[0, 2, 1] = eijk[2, 1, 0] = eijk[1, 0, 2] = -1

        # First we construct great circles defined by every possible pair of
        # points and represented as normal vectors

        # cross-products, NxNx3
        circles = np.einsum("ijk,uj,vk->uvi", eijk, xyz, xyz, optimize=True)
        # norms, i.e. arc sine lengths, NxN
        norms = np.sqrt(np.einsum("uvi,uvi->uv", circles, circles, optimize=True))

        # Remove redundant entries corresponding to
        # the symmetry on point swapping
        norms = np.tril(norms)

        # Pairs with angular separation larger than 10 arcsec, NxN
        norm_idx = norms > 10 / 206265

        circles[norms == 0, :] = 0
        # normalize normals to great circles, NxNx3
        circles[norms > 0, :] /= norms[norms > 0, np.newaxis]

        # Sets of points along great circles
        cidxs = []

        # Now let's cycle along first point of circle, N iterations
        for i in range(len(xyz)):
            # Here first index means second point of circle
            # while second one represent all points of dataset

            # sine distance from the great circle, NxN
            sindists = np.einsum("vi,ki->vk", circles[i], xyz, optimize=True)
            # Good distances from great circles, NxN
            sin_idx = np.abs(sindists) < 5 / 206265

            # The same but only for circles formed by pairs distant enough
            good_idx = norm_idx[i, :, np.newaxis] & sin_idx

            # Numbers of good points along each great circle, N
            nps = np.einsum("uv->u", good_idx.astype(np.int8), optimize=True)
            np_idx = nps >= 5

            # Tracklet candidates
            cidxs += list(sin_idx[np_idx])

        if len(cidxs) == 0:
            return pdf

        uniq = np.unique(cidxs, axis=0)
        # Sort by the (decreasing) length of tracklet candidates
        aidx = np.argsort([-np.sum(_) for _ in uniq])

        used = np.zeros(len(ra), dtype=bool)

        index_tracklet = 0
        for cidx in uniq[aidx]:
            # First we need to reject the tracklets
            # mostly superseded by longer ones
            if np.sum(cidx[~used]) < 5:
                # TODO: should we try to greedily merge the extra points
                # into already known tracks?
                continue

            # Quick and dirty way to sort the tracklet consecutively, in the
            # direction defined by its two first points

            # great circles formed by first point of a
            # tracklet and and all other points
            scircles = np.einsum("ijk,j,vk->vi", eijk, xyz[cidx][0], xyz, optimize=True)

            # dot products between (0-1) circle of a tracklet and all others,
            # we will use it as a sort of distance along the great circle
            dots = np.einsum("i,vi->v", scircles[cidx][1], scircles, optimize=True)

            # sort the tracklet in increasing order
            aidx = np.argsort(dots[cidx])

            # circle formed by first and last points of a sorted tracklet
            circle0 = np.cross(xyz[cidx][aidx[0]], xyz[cidx][aidx[-1]])

            # Normalized circle
            circle0 /= np.sqrt(np.sum(circle0 * circle0))

            # Sine distances of all points from that circle
            sindists = np.dot(xyz, circle0)

            # Greedily capture more (or restrict to less) points using
            # polynomial correction and smaller acceptable residuals from
            # corrected (curved) trail
            for _ in range(10):
                # TODO: robust fitting here?..
                p = np.polyfit(dots[cidx], sindists[cidx], 2)
                model = np.polyval(p, dots)

                new_cidx = np.abs(sindists - model) < 1 / 206265

                if np.sum(new_cidx) > 1:
                    # Exclude the cases when first or last point is too much
                    # separated from the rest
                    sort_idx = np.argsort(dots[new_cidx])
                    sort_ids = np.where(new_cidx)[0][sort_idx]

                    # Pairwise distances
                    dists = np.arccos(
                        np.einsum(
                            "vi,vi->v",
                            xyz[new_cidx][sort_idx][1:, :],
                            xyz[new_cidx][sort_idx][:-1, :],
                        )
                    )

                    # Here we check whether first/last distance is more
                    # than 10 times longer than the rest
                    if dists[0] > 10 / 11 * np.sum(dists):
                        # Exclude first point
                        new_cidx[sort_ids[0]] = False
                    elif dists[-1] > 10 / 11 * np.sum(dists):
                        # Exclude last point
                        new_cidx[sort_ids[-1]] = False

                if np.all(new_cidx == cidx):
                    break
                else:
                    # Greedily extend the tracklet, never dropping any point
                    # cidx |= new_cidx
                    # Conservatively extend the tracklet, excluding points
                    # too far from the fit
                    cidx = new_cidx

                if np.sum(cidx) < 3:
                    # We do not have enough points, meaning the fit diverged
                    break

            merge = None
            if np.sum(cidx) < 5:
                # The tracklet is too short after applying smaller acceptable
                # residuals, let's reject it
                continue
            elif np.sum(cidx & used) > 2:
                # More than 2 common points with some existing
                # trail - let's merge?..
                # TODO: something more clever for merge strategy?..
                merge = tracklet_names[cidx & used].unique()[0]
            elif np.sum(cidx & used):
                # Looks like two crossing tracklets, no need to merge
                pass

            # We will claim for new tracklet only points unused by existing
            # ones, thus making point assignment unique
            # unused_cidx = cidx & ~used

            used[cidx] = True

            tracklet_positions = cidx

            if merge is not None:
                tracklet_names[tracklet_positions] = merge
            else:
                tracklet_names[tracklet_positions] = "TRCK_{}_{:02d}".format(
                    time_str, index_tracklet
                )
                index_tracklet += 1

        return pdf.assign(tracklet=tracklet_names)

    # extract tracklet information - beware there could be duplicated rows
    # so we use dropDuplicates to avoid these.
    df_trck = (
        df_filt_tracklet.cache()
        .dropDuplicates(["jd", "xpos", "ypos"])
        .groupBy("jd")
        .apply(extract_tracklet_number)
        .select(["candid", "tracklet"])
        .filter(F.col("tracklet") != "")
    )

    return df_trck


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ["FINK_HOME"]
    globs["ztf_alert_sample"] = os.path.join(root, "datasim/tracklet_alerts")

    # Run the Spark test suite
    spark_unit_tests(globs, withstreaming=True)
