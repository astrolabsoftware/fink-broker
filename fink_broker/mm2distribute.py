import os

from fink_mm.distribution.distribution import grb_distribution_stream


def mm2distribute(spark, config, args):

    mm_data_path = config["PATH"]["online_grb_data_prefix"]
    kafka_broker = config["DISTRIBUTION"]["kafka_broker"]
    username_writer = config["DISTRIBUTION"]["username_writer"]
    password_writer = config["DISTRIBUTION"]["password_writer"]

    year, month, day = args.night[0:4], args.night[4:6], args.night[6:8]
    basepath = os.path.join(mm_data_path, "online", f"year={year}/month={month}/day={day}")
    checkpointpath_mm = os.path.join(mm_data_path, "mm_distribute_checkpoint")

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
