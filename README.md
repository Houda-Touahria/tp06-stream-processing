تفضلي هذا النص، انسخيه والصقيه مباشرة في GitHub (بعد الضغط على **Add a README** في صفحة المستودع) ثم اضغطي **Commit changes**:

```markdown
# TP06: Real-Time Stream Processing (Kafka, Flink SQL & Spark)

End-to-end stream processing pipeline built for real-time campus event analytics. The project ingests simulated student check-in/check-out events through Apache Kafka and processes them using both **Apache Flink SQL** and **PySpark Structured Streaming**.

## Architecture Overview

1. **Data Ingestion (`producer.py` -> Kafka):**
   - Publishes JSON-formatted campus events (`eventId`, `studentId`, `eventType`, `department`, `eventTime`) to the `campus-events` topic.
2. **Stream Processing Engine 1 (Apache Flink SQL):**
   - Consumes messages via `flink-sql-connector-kafka`.
   - Parses ISO string timestamps into SQL `TIMESTAMP(3)` with a 30-second `WATERMARK` to handle late events.
   - Computes 1-minute `TUMBLE` window aggregations for student check-ins per department.
3. **Stream Processing Engine 2 (PySpark Structured Streaming):**
   - Consumes the same `campus-events` stream using micro-batch execution.
   - Runs streaming queries with fault-tolerant checkpointing:
     - **Department Window Counts:** 1-minute tumbling window aggregation (`update` mode) with a 30s watermark.
     - **Late Arrivals Detection:** Converts UTC timestamps to `Africa/Algiers` (`UTC+1`) and filters check-ins occurring after `08:15:00` (`append` mode).

## Project Structure

```text
├── producer.py              # Simulated event stream generator
├── flink_window.sql         # Flink SQL DDL and tumbling window queries
├── spark_stream.py          # PySpark Structured Streaming job (windowed counts)
├── spark_stream1.py         # PySpark Structured Streaming job (counts + late arrivals)
├── screenshots/             # Execution outputs and verification captures
└── .gitignore               # Excludes virtual environments, checkpoints, and cache

```

## Setup & Execution

### 1. Start Infrastructure

Ensure the Docker containers for Kafka (`tp05-kafka`) and Flink (`JobManager` and `TaskManager`) are running on the shared network.

### 2. Run the Event Producer

Start streaming events to the `campus-events` topic:

```bash
python producer.py

```

### 3. Run Flink SQL Pipeline

Access the Flink SQL client and execute the table definitions and window queries in `flink_window.sql`:

```bash
docker exec -it jobmanager ./bin/sql-client.sh

```

### 4. Run Spark Structured Streaming

Execute the PySpark job (automatically fetches the Kafka connector package):

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_stream1.py

```

## Implementation Notes

* Containers share the network namespace to avoid advertised-listener resolution issues on `localhost:9092`.
* Shuffle partitions in Spark are tuned (`spark.sql.shuffle.partitions=3`) to reduce local execution overhead.

```

---

**ملاحظة بعد الحفظ على GitHub:**
بما أنكِ أنشأتِ `README.md` من المتصفح مباشرة، نفّذي `git pull` في نافذة CMD على جهازكِ لاحقاً إذا أردتِ مزامنة المجلد المحلي مع آخر نسخة على GitHub.

```
