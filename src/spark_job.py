
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType


BUCKET_NAME = os.environ.get("CLICKSTREAM_BUCKET", "project-bda-data-123")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
SESSION_GAP_MIN = int(os.environ.get("SESSION_GAP_MIN", "30"))

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

RAW_PATH = f"s3a://{BUCKET_NAME}/raw"
PROCESSED_PATH = f"s3a://{BUCKET_NAME}/processed"


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("RetailRocketClickstream")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", REGION)
    )

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )
    else:
        
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_events(spark: SparkSession):
    events_schema = StructType([
        StructField("timestamp", LongType(), False),
        StructField("visitorid", LongType(), False),
        StructField("event", StringType(), False),
        StructField("itemid", LongType(), False),
        StructField("transactionid", LongType(), True),
    ])

    events = (
        spark.read.csv(f"{RAW_PATH}/events.csv", header=True, schema=events_schema)
        .withColumn("event_ts", (F.col("timestamp") / 1000).cast(TimestampType()))
        .withColumn("year", F.year("event_ts"))
        .withColumn("month", F.month("event_ts"))
        .withColumn("day", F.dayofmonth("event_ts"))
    )
    return events


def sessionize(events):
    w_visitor = Window.partitionBy("visitorid").orderBy("event_ts")

    sessioned = (
        events
        .withColumn(
            "gap_min",
            (F.col("timestamp") - F.lag("timestamp").over(w_visitor)) / (1000 * 60),
        )
        .withColumn(
            "is_new_session",
            F.when(F.col("gap_min").isNull(), 1)
             .when(F.col("gap_min") > SESSION_GAP_MIN, 1)
             .otherwise(0),
        )
        .withColumn(
            "session_seq",
            F.sum("is_new_session").over(w_visitor.rowsBetween(Window.unboundedPreceding, 0)),
        )
        .withColumn("session_id", F.concat_ws("_", F.col("visitorid"), F.col("session_seq")))
        .drop("gap_min", "is_new_session", "session_seq")
    )
    return sessioned


def build_sessions_agg(sessioned):
    return (
        sessioned.groupBy("visitorid", "session_id", "year", "month", "day")
        .agg(
            F.min("event_ts").alias("session_start"),
            F.max("event_ts").alias("session_end"),
            F.count("*").alias("event_count"),
            F.countDistinct("itemid").alias("distinct_items"),
            F.sum(F.when(F.col("event") == "view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event") == "addtocart", 1).otherwise(0)).alias("addtocarts"),
            F.sum(F.when(F.col("event") == "transaction", 1).otherwise(0)).alias("transactions"),
        )
        .withColumn("converted", (F.col("transactions") > 0).cast("int"))
    )


def build_daily_funnel(sessioned):
    return (
        sessioned.groupBy("year", "month", "day")
        .agg(
            F.countDistinct(F.when(F.col("event") == "view", F.col("visitorid"))).alias("viewers"),
            F.countDistinct(F.when(F.col("event") == "addtocart", F.col("visitorid"))).alias("carters"),
            F.countDistinct(F.when(F.col("event") == "transaction", F.col("visitorid"))).alias("buyers"),
            F.count(F.when(F.col("event") == "view", 1)).alias("total_views"),
            F.count(F.when(F.col("event") == "addtocart", 1)).alias("total_addtocarts"),
            F.count(F.when(F.col("event") == "transaction", 1)).alias("total_transactions"),
        )
        .withColumn("event_date", F.to_date(F.concat_ws("-", "year", "month", "day")))
        .withColumn("view_to_cart_rate", F.col("carters") / F.col("viewers"))
        .withColumn("cart_to_purchase_rate", F.col("buyers") / F.col("carters"))
    )


def build_top_items(sessioned):
    return (
        sessioned.groupBy("itemid")
        .agg(
            F.countDistinct(F.when(F.col("event") == "view", F.col("visitorid"))).alias("viewers"),
            F.countDistinct(F.when(F.col("event") == "addtocart", F.col("visitorid"))).alias("carters"),
            F.countDistinct(F.when(F.col("event") == "transaction", F.col("visitorid"))).alias("buyers"),
        )
    )


def main():
    spark = build_spark_session()
    print("Spark version:", spark.version)

    events = load_events(spark)
    print("Row count:", events.count())

    sessioned = sessionize(events)
    sessions_agg = build_sessions_agg(sessioned)
    daily_funnel = build_daily_funnel(sessioned)
    top_items = build_top_items(sessioned)

    daily_funnel.show(10)
    top_items.orderBy(F.desc("buyers")).show(10)

    sessions_agg.coalesce(4).write.mode("overwrite").parquet(f"{PROCESSED_PATH}/sessions")
    daily_funnel.coalesce(1).write.mode("overwrite").parquet(f"{PROCESSED_PATH}/daily_funnel")
    top_items.coalesce(1).write.mode("overwrite").parquet(f"{PROCESSED_PATH}/top_items")

    print("Done! Files written to:", PROCESSED_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
