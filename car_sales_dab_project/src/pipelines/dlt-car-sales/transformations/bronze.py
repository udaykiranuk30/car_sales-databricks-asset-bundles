import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Schema definition for the Car Sales fact table.
# This StructType outlines the expected structure of incoming streaming data
# for the Delta Live Tables pipeline. It defines each column name, data type,
# and nullability. The schema is used during ingestion to enforce data
# consistency, ensure proper type casting, and support reliable processing
# through the bronze → silver → gold layers.
#
# Note:
# - sale_date is defined as TimestampType to allow temporal processing,
#   windowing, and time-based aggregations.
# - Nullable fields are allowed to accommodate intentionally injected bad data,
#   such as null prices, missing VINs, or invalid references.
# - This schema aligns with the star-schema design where customer_id,
#   branch_id, and dealer_id act as foreign keys for dimension tables.

car_sales_schema = StructType([
    StructField("sale_id", StringType(), True),
    StructField("sale_date", TimestampType(), True),
    StructField("car_manufacturer", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("fuel_type", StringType(), True),
    StructField("transmission_type", StringType(), True),
    StructField("vin", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("payment_mode", StringType(), True),
    StructField("branch_id", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("dealer_id", IntegerType(), True)
])

# Fetch parameter from pipeline configuration
source = spark.conf.get("source")

# Bronze Layer: Raw Streaming Ingestion
# -------------------------------------
# This table ingests raw car sales data using Auto Loader in streaming mode.
# Auto Loader (cloudFiles) incrementally processes new CSV files that land in
# the source directory defined by the pipeline configuration.
#
# Key points:
# - Streaming ingestion ensures files are captured in real time as they arrive.
# - The predefined schema (car_sales_schema) enforces column structure at ingest.
# - Auto Loader manages schema inference, tracking, and fault tolerance.
# - Unity Catalog requires using `_metadata.file_path` instead of input_file_name()
#   to capture the source file path for lineage and audit purposes.
# - Additional metadata columns such as IngestionTime provide traceability for
#   downstream cleansing and enrichment in the silver and gold layers.
#
# This bronze table stores raw, uncleaned data as the foundation for the pipeline.
@dlt.table(
    name="carsales_bronze",
    comment="Raw Data Ingestion of Car Sales Data into Bronze Table"
)
def carsales_bronze():
    return (
        spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "csv")\
                .option("header",True)\
                    .schema(car_sales_schema)\
                        .load(f"{source}/carsales_yearwise")\
                            .withColumn("IngestionTime",F.current_timestamp())\
                                .withColumn("FileSource",F.col("_metadata.file_path"))
    )

# Bronze Layer: Dealers Dimension (Batch Ingestion)
# -------------------------------------------------
# This table ingests the Dealers dimension as a batch dataset since the
# dealers file is static and does not arrive continuously like streaming data.
#
# Key points:
# - Uses spark.read (batch mode) because dimension data is typically small,
#   stable, and does not require streaming semantics.
# - Auto Loader is not required for static files, making ingestion simpler
#   and more efficient for one-time or infrequently updated datasets.
# - `_metadata.file_path` is used to capture lineage details, as mandated by
#   Unity Catalog when tracking the source file for audit and debugging.
# - IngestionTime provides a timestamp that identifies when the data was
#   loaded into the bronze layer and is useful for debugging and downstream
#   transformations.
#
# This bronze table forms the raw version of the Dealers dimension that will
# later be cleaned and standardized in the Silver layer.
@dlt.table(
    name="dealers_bronze",
    comment=" Raw Data Ingestion of Dealers Data "
)
def dealers_bronze():
    return(
        spark.read.format("csv")\
            .option("header",True)\
                .option("inferSchema",True)\
                    .load(f"{source}/dealers")\
                        .withColumn("IngestionTime",F.current_timestamp())\
                                .withColumn("FileSource",F.col("_metadata.file_path"))
    )

# Bronze Layer: Branches Dimension (Batch Ingestion)
# --------------------------------------------------
# This table loads the Branches dimension using batch ingestion since the
# data is static and does not arrive incrementally like the streaming fact
# data. Batch mode is ideal here because the dataset is relatively small,
# stable, and does not require the overhead of streaming with checkpoints.
#
# Key points:
# - spark.read is used to ingest a static CSV file, ensuring a simple and
#   efficient loading process for dimension data.
# - Unity Catalog requires using `_metadata.file_path` to capture the source
#   file path for lineage, auditability, and debugging purposes.
# - IngestionTime provides a reference timestamp that indicates when this
#   dataset was loaded into the bronze layer.
#
# This bronze table represents the raw Branches dimension and will be cleaned,
# standardized, and joined with facts in downstream Silver and Gold layers.
@dlt.table(
    name="branches_bronze",
    comment=" Raw Data Ingestion of Branches Data "
)
def branches_bronze():
    return(
        spark.read.format("csv")\
            .option("header",True)\
                .option("inferSchema",True)\
                    .load(f"{source}/branches")\
                        .withColumn("IngestionTime",F.current_timestamp())\
                                .withColumn("FileSource",F.col("_metadata.file_path"))
    )

# Bronze Layer: Customers Dimension (Batch Ingestion)
# ---------------------------------------------------
# This table ingests the Customers dimension using batch processing, as the
# underlying data is static and does not arrive in a streaming fashion. Batch
# ingestion is the ideal choice for dimension datasets that are relatively
# stable, small in size, and updated infrequently.
#
# Key points:
# - spark.read is used instead of streaming since the customers data does not
#   require incremental ingestion or checkpointing.
# - `_metadata.file_path` is leveraged to capture the file source for lineage
#   and auditability, which is the recommended approach under Unity Catalog.
# - IngestionTime is added to support traceability, providing insight into when
#   the dataset was brought into the bronze layer.
#
# This bronze table forms the raw Customers dimension and will later be
# validated, cleaned, and enriched in the Silver layer for downstream joins
# with fact tables.
@dlt.table(
    name="customers_bronze",
    comment=" Raw Data Ingestion of Customers Data "
)
def customers_bronze():
    return(
        spark.read.format("csv")\
            .option("header",True)\
                .option("inferSchema",True)\
                    .load(f"{source}/customers")\
                        .withColumn("IngestionTime",F.current_timestamp())\
                                .withColumn("FileSource",F.col("_metadata.file_path"))
    )
    
                

            




