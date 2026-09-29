#!/usr/bin/env python
# Copyright 2025-2026 AstroLab Software
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
"""Run the SLSN classifier, and push data to Slack."""

import argparse
import joblib
import numpy as np

from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.common.logging_utils import get_fink_logger, inspect_application
from fink_broker.ztf.hbase_utils import push_full_df_to_hbase

from fink_utils.photometry.conversion import mag2fluxcal_snana

from fink_filters.ztf.classification import extract_fink_classification
from fink_filters.ztf.filter_anomaly_notification.filter_utils import msg_handler_slack
from fink_filters.ztf.filter_anomaly_notification.filter_utils import (
    get_data_permalink_slack,
)

from fink_science.ztf.superluminous.slsn_classifier import (
    get_regalade_photoz,
    get_ebv,
)

from fink_science.ztf.superluminous.processor import get_and_format

import fink_science.ztf.superluminous.kernel as kern


def ntrend_changes(cflux, csigflux, cfid, k=3):
    """Returns the mean (per band) number of time the light curve abruptly changed overall trend.

    (e.g. light curve was rising and abruptly goes down)
    """
    n = []
    for band in np.unique(cfid):
        mask = (cfid == band) & ~np.isnan(cflux)
        x = cflux[mask]
        err = np.array(csigflux[mask], dtype=float)

        dx = np.diff(x)
        sig = np.sqrt(err[:-1] ** 2 + err[1:] ** 2)
        valid = np.abs(dx) > k * sig
        n_turns = np.sum(np.diff(np.sign(dx[valid])) != 0)

        n.append(n_turns)

    return np.mean(n)


def _regalade_photoz(row):
    """Compute the REGALADE-refined host photo-z for a row, for display only.

    Notes
    -----
    This is informational only -- no brightness veto is computed here.
    The real-time classifier (`superluminous_score` in
    fink_science.ztf.superluminous.processor) already applies the
    peak-absolute-magnitude veto using the object's *full* light-curve
    history (fetched via `get_and_format`) before a candidate is ever
    scored. Recomputing it here from just the alert's own
    `prv_candidates` would use strictly worse data -- `prv_candidates`
    is a rolling ~30-day window, which can be much shorter than the time
    since the object's first detection (`jdstarthist`), silently missing
    an earlier, brighter peak and producing a wrong (too faint) veto.

    Parameters
    ----------
    row: dict
        Pandas DataFrame row as dictionary. Must include ra, dec, and the
        REGALADE columns already attached to the alert (regalade_ra,
        regalade_dec, R1, R2, PA, z, ezin).

    Returns
    -------
    photoz, photozerr: float
        REGALADE photo-z and its uncertainty. NaN if no host is found.
    ebv: float
        Milky Way E(B-V) extinction at (ra, dec).
    """
    ebv = get_ebv(np.array([row.ra]), np.array([row.dec]))[0]

    photoz, photozerr = get_regalade_photoz(
        np.array([row.ra]),
        np.array([row.dec]),
        np.array([row.regalade_ra]),
        np.array([row.regalade_dec]),
        np.array([row.R1]),
        np.array([row.R2]),
        np.array([row.PA]),
        np.array([row.z]),
        np.array([row.ezin]),
    )
    photoz, photozerr = photoz[0], photozerr[0]

    return photoz, photozerr, ebv


def append_slack_messages(slack_data: list, row: dict, slack_token_env: str) -> None:
    """Append messages to list for Slack distribution.

    Parameters
    ----------
    slack_data: list
        List containing all Slack messages. Each element
        is a message (string).
    row: dict
        Pandas DataFrame row as dictionary. Contains
        Fink data.
    slack_token_env: str
        Environment variable that has the Slack bot token
    """
    if row.tns == "":
        t0 = "No TNS classification"
    else:
        t0 = f"TNS classification: {row.tns}"

    t1 = f"Fink: <https://ztf.fink-portal.org/{row.objectId}|{row.objectId}>"
    t1bis = f"Fritz: <https://fritz.science/source/{row.objectId}|{row.objectId}>"
    t2 = f"Score: {round(row.slsn_score, 3)}"

    cutout, curve, cutout_perml, curve_perml = get_data_permalink_slack(
        row.objectId, slack_token_env=slack_token_env
    )

    photoz, photozerr, ebv = _regalade_photoz(row)
    t3 = f"E(B-V) = {ebv:.3f}"
    t4 = ""
    if photoz == photoz:
        t4 = f"REGALADE photo-z = {photoz:.3f} +- {photozerr:.3f}"

    curve.seek(0)
    cutout.seek(0)
    cutout_perml = f"<{cutout_perml}|{' '}>"
    curve_perml = f"<{curve_perml}|{' '}>"
    slack_data.append(
        f"""==========================
    {t0}
    {t1}
    {t1bis}
    {t2}
    {t3}
    {t4}
    {cutout_perml}{curve_perml}"""
    )


def apply_cuts(unique):
    """Apply some additional cuts based on the full light curves.

    Notes
    -----
    These cuts should later be integrated directly to the model.

    Parameters
    ----------
    unique: pd.DataFrame
        DataFrame containing data for SLSN (unique objectIds)

    Returns
    -------
    unique_filtered: pd.DataFrame
        `unique`, filtered down to objects passing the cuts below. Has
        the same columns as `unique`, so it can be iterated directly to
        build the Slack detail messages -- keeping the summary count and
        the number of messages sent always in sync.
    summary: pd.DataFrame
        DataFrame with objectId and score to be printed on Slack
    """
    unique_lcs = get_and_format(list(unique["objectId"]))

    conversion = unique_lcs[["cmagpsf", "csigmapsf"]].apply(
        lambda x: np.transpose(
            [mag2fluxcal_snana(*i) for i in zip(x["cmagpsf"], x["csigmapsf"])]
        ),
        axis=1,
    )

    unique_lcs["cflux"] = conversion.apply(lambda x: x[0])
    unique_lcs["csigflux"] = conversion.apply(lambda x: x[1])
    unique_lcs["ntrends"] = unique_lcs.apply(
        lambda x: ntrend_changes(x["cflux"], x["csigflux"], x["cfid"]), axis=1
    )

    # Check that the phtometry doesn"t vary abruptly too often
    n_trends_cut = np.array(unique_lcs["ntrends"] <= 2)

    # Check that the object isn"t too old (likely AGN or bad photometry, and if not should have been catch way before)
    duration_cut = np.array(unique_lcs["cjd"].apply(np.ptp) < 500)

    # No brightness veto here: `superluminous_score` already applies it
    # upstream using the object's full light-curve history, before a
    # candidate is ever scored -- see `_regalade_photoz`'s docstring for
    # why recomputing it here (from just the alert's own prv_candidates)
    # would be both redundant and wrong.

    # Apply cuts
    unique_filtered = unique[n_trends_cut & duration_cut]

    summary = (unique_filtered[["objectId", "slsn_score"]]).sort_values(
        "slsn_score", ascending=False
    )
    summary = summary.reset_index(drop=True)

    return unique_filtered, summary


def main():
    """Extract probabilities from the SLSN model, and send results to Slack."""
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="SLSN_{}".format(args.night), shuffle_partitions=2)

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)
    ""
    # Connect to the aggregated science database
    path = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, args.night[:4], args.night[4:6], args.night[6:8]
    )
    df = load_parquet_files(path)

    # Add classification
    cols = [
        "cdsxmatch",
        "roid",
        "mulens",
        "snn_snia_vs_nonia",
        "snn_sn_vs_all",
        "rf_snia_vs_nonia",
        "candidate.ndethist",
        "candidate.drb",
        "candidate.classtar",
        "candidate.jd",
        "candidate.jdstarthist",
        "rf_kn_vs_nonkn",
        "tracklet",
    ]
    df = df.withColumn("classification", extract_fink_classification(*cols))

    clf = joblib.load(kern.classifier_path)
    optimal_threshold = clf.optimal_threshold

    df_filt = df.filter(df["slsn_score"] >= optimal_threshold)

    cols_ = [
        "objectId",
        "candidate.ra",
        "candidate.dec",
        "classification",
        "slsn_score",
        "candidate.ndethist",
        "candidate.jdstarthist",
        "candidate.jd",
        "tns",
        "regalade_ra",
        "regalade_dec",
        "R1",
        "R2",
        "PA",
        "z",
        "ezin",
    ]

    pdf = df_filt.select(cols_).toPandas()

    if not pdf.empty:
        pdf = pdf.sort_values("slsn_score", ascending=False)

        unique = pdf.loc[pdf.groupby("objectId")["ndethist"].idxmax()]

        # Apply some additional cuts based on the full light curves.
        # These cuts should later be integrated directly to the model.
        unique_filtered, summary = apply_cuts(unique)

        init_msg = f"Number of unique candidates for the night {args.night}: {len(unique_filtered)}.\n\n{summary}"
    else:
        unique_filtered = pdf
        init_msg = f"No candidates found for the night {args.night}"

    envs = ["ANOMALY_SLACK_TOKEN", "SLSN_SLACK_ZTF", "SLSN_SLACK_OSCAR"]
    channels = ["#bot_slsn", "#slsn-candidates", "#slsn-candidates"]

    for slack_token_env, channel in zip(envs, channels):
        slack_data = []
        for _, row in unique_filtered.iterrows():
            append_slack_messages(slack_data, row, slack_token_env)

        msg_handler_slack(
            slack_data, channel, init_msg, slack_token_env=slack_token_env
        )

    # Send to HBase
    # Need to recompute a Spark DF because there are cuts applied later on Pandas DF
    if not pdf.empty:
        df_hbase = df.filter(df["objectId"].isin(unique_filtered["objectId"].to_list()))

        # Drop images
        df_hbase = (
            df_hbase.drop("cutoutScience")
            .drop("cutoutTemplate")
            .drop("cutoutDifference")
        )

        # Row key
        row_key_name = "jd_objectId"

        # push data to HBase
        push_full_df_to_hbase(
            df_hbase,
            row_key_name=row_key_name,
            table_name=args.science_db_name + ".slsn",
            catalog_name=args.science_db_catalogs,
        )


if __name__ == "__main__":
    main()
