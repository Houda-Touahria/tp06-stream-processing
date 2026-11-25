from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, from_utc_timestamp, hour, minute, second
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = (
    SparkSession.builder
    .appName("CampusRealTimeAnalytics")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .config("spark.sql.shuffle.partitions", "3")  # لتسريع المعالجة على الجهاز المحلي
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("eventId", IntegerType(), True),
    StructField("studentId", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("department", StringType(), True),
    StructField("eventTime", StringType(), True)
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "campus-events")
    .option("startingOffsets", "earliest")
    .load()
)

json_events = raw.selectExpr("CAST(value AS STRING) AS json")

parsed = (
    json_events
    .select(from_json(col("json"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", to_timestamp(col("eventTime")))
    .filter(col("event_ts").isNotNull())
)

checkins = parsed.filter(col("eventType") == "check-in")

# الاستعلام الأول (Task 3): حساب الحضور حسب القسم في نوافذ دقيقة واحدة
counts = (
    checkins
    .withWatermark("event_ts", "30 seconds")
    .groupBy(
        window(col("event_ts"), "1 minute"),
        col("department")
    )
    .count()
)

query_counts = (
    counts.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "./checkpoint/campus-events")
    .start()
)

# الاستعلام الثاني (Task 5): تحديد المتأخرين بعد 08:15:00 بتوقيت الجزائر المحلي (UTC+1)
local_checkins = checkins.withColumn("local_ts", from_utc_timestamp(col("event_ts"), "Africa/Algiers"))

late_arrivals = (
    local_checkins
    .filter(
        (hour(col("local_ts")) > 8) |
        ((hour(col("local_ts")) == 8) & (minute(col("local_ts")) > 15)) |
        ((hour(col("local_ts")) == 8) & (minute(col("local_ts")) == 15) & (second(col("local_ts")) > 0))
    )
    .select("studentId", "department", "eventTime", "local_ts")
)

query_late = (
    late_arrivals.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "./checkpoint/late-arrivals")
    .start()
)

spark.streams.awaitAnyTermination()