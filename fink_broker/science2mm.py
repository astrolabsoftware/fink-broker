from fink_mm.ztf_join_gcn import ztf_join_gcn_stream, DataMode
from astropy.time import Time
from datetime import timedelta

from fink_broker.sparkUtils import connect_to_raw_database
from pyspark.sql.streaming import StreamingQuery
import argparse
import configparser


def science2mm(
    args: argparse.Namespace,
    config: configparser.ConfigParser,
    gcndatapath: str,
    scitmpdatapath: str,
) -> StreamingQuery:
    """
    Launch the GCN x ZTF cross-match pipeline.

    Parameters
    ----------
    args : argparse.Namespace
        arguments from the fink command line
    config : configparser.ConfigParser
        config entries of fink_mm
    gcndatapath : str
        path where are stored the GCN x ZTF data
    scitmpdatapath : str
        path where are stored the fink science stream

    Returns
    -------
    StreamingQuery
        the fink_mm query, used by the caller
    """
    ztf_dataframe = connect_to_raw_database(
        scitmpdatapath, scitmpdatapath, latestfirst=False
    )
    gcn_dataframe = connect_to_raw_database(
        gcndatapath,
        gcndatapath,
        latestfirst=False,
    )
    # keep gcn emitted between the last day time and the end of the current stream (17:00 Paris Time)
    cur_time = Time(f"{args.night[0:4]}-{args.night[4:6]}-{args.night[6:8]}")
    last_time = cur_time - timedelta(hours=7)  # 17:00 Paris time yesterday
    end_time = cur_time + timedelta(hours=17)  # 17:00 Paris time today
    gcn_dataframe = gcn_dataframe.filter(
        f"triggerTimejd >= {last_time.jd} and triggerTimejd < {end_time.jd}"
    )
    df_multi_messenger, _ = ztf_join_gcn_stream(
        DataMode.STREAMING,
        ztf_dataframe,
        gcn_dataframe,
        gcndatapath,
        args.night,
        int(config["ADMIN"]["NSIDE"]),
        config["HDFS"]["host"],
        ast_dist=float(config["PRIOR_FILTER"]["ast_dist"]),
        pansstar_dist=float(config["PRIOR_FILTER"]["pansstar_dist"]),
        pansstar_star_score=float(config["PRIOR_FILTER"]["pansstar_star_score"]),
        gaia_dist=float(config["PRIOR_FILTER"]["gaia_dist"]),
    )

    mmdatapath = config["PATH"]["online_grb_data_prefix"]
    checkpointpath_mm_tmp = args.online_data_prefix + "/mm_checkpoint/{}".format(
        args.night
    )

    query_mm = (
        df_multi_messenger.writeStream.outputMode("append")
        .format("parquet")
        .option("checkpointLocation", checkpointpath_mm_tmp)
        .option("path", mmdatapath)
        .partitionBy("year", "month", "day")
        .trigger(processingTime="{} seconds".format(args.tinterval))
        .start()
    )

    return query_mm
