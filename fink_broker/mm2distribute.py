import os
import time

from fink_mm.distribution.distribution import grb_distribution_stream
from fink_broker.loggingUtils import get_fink_logger

def mm2distribute(spark, config, args):

    mm_data_path = config["PATH"]["online_grb_data_prefix"]
    kafka_broker = config["DISTRIBUTION"]["kafka_broker"]
    username_writer = config["DISTRIBUTION"]["username_writer"]
    password_writer = config["DISTRIBUTION"]["password_writer"]

    year, month, day = args.night[0:4], args.night[4:6], args.night[6:8]
    basepath = os.path.join(mm_data_path, "online", f"year={year}/month={month}/day={day}")
    checkpointpath_mm = os.path.join(mm_data_path, "mm_distribute_checkpoint")

    logger = get_fink_logger()
    wait = 5
    while True:
        try:
            logger.info("successfully connect to the MM database")
            # force the mangrove columns to have the struct type
            static_df = spark.read.parquet(basepath)

            path = basepath
            df_grb_stream = (
                spark.readStream.format("parquet")
                .schema(static_df.schema)
                .option("basePath", basepath)
                .option("path", path)
                .option("latestFirst", True)
                .load()
            )
            break

        except Exception:
            logger.info(f"Exception occured: wait: {wait}", exc_info=1)
            time.sleep(wait)
            wait *= 1.2 if wait < 60 else 1
            continue

    stream_distribute_list = grb_distribution_stream(
        df_grb_stream,
        static_df,
        checkpointpath_mm,
        args.tinterval,
        kafka_broker,
        username_writer,
        password_writer,
    )

    return stream_distribute_list
