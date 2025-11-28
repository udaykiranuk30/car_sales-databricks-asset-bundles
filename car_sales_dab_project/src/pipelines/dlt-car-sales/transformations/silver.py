import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Silver Layer: Dealers Dimension (Validated and Cleansed)
# --------------------------------------------------------
# This table processes the raw Dealers data from the Bronze layer and applies
# data quality rules using DLT expectations. Because the Dealers dataset is
# static and was ingested in batch mode, this transformation uses `dlt.read()`
# instead of a streaming read.
#
# Key points:
# - Enforces data quality constraints on dealer_id, branch_id, manufacturer,
#   and dealer_name using `expect_or_drop` to ensure only valid records flow
#   into downstream layers.
# - Provides a clean, reliable, and analytics-ready dimension table.
# - This Silver table serves as the standardized reference for dimension lookups
#   when enriching Car Sales Fact data in the Gold layer.
#
# The goal is to ensure that only high-quality and structurally valid dealer
# records participate in downstream transformation and analytics tasks.
@dlt.table(
    name="dealers_silver",
    comment="The Enriched Data of Car dealers with Valid Expectations and Data Quality Checks"
)
@dlt.expect_or_drop("valid_dealer_id", "dealer_id IS NOT NULL AND dealer_id > 0")
@dlt.expect_or_drop("valid_branch_id", "branch_id IS NOT NULL AND branch_id > 0")
@dlt.expect_or_drop("valid_car_manufacturer", "car_manufacturer IS NOT NULL AND car_manufacturer != ''")
@dlt.expect_or_drop("valid_dealer_name", "dealer_name IS NOT NULL AND dealer_name != ''")
def dealers_silver():
    return(
        dlt.read("dealers_bronze")
    )

# Silver Layer: Customers Dimension (Validated and Cleansed)
# ----------------------------------------------------------
# This table transforms the raw Customers data from the Bronze layer into a
# high-quality, analytics-ready dimension. Because the Customers dataset is
# static and was ingested in batch mode, this transformation uses `dlt.read()`
# rather than a streaming read.
#
# Key points:
# - Applies data quality rules using DLT `expect_or_drop` to enforce valid
#   customer records. Invalid entries (missing IDs, bad emails, invalid phone
#   numbers, empty names, etc.) are removed at this stage.
# - Ensures the Customers dimension contains only clean, trustworthy data that
#   can be safely joined with the Car Sales Fact table in the Gold layer.
# - Produces a structured, consistent representation of customer information
#   that supports downstream analytics, dashboards, and business logic.
#
# This Silver layer provides a reliable cleaned version of the Customers
# dimension for enrichment and analytics across the pipeline.
@dlt.table(
    name="customers_silver",
    comment="The Enriched Data of Customers with Valid Expectations and Data Quality Checks"
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL AND customer_id > 0")
@dlt.expect_or_drop("valid_customer_name", "customer_name IS NOT NULL AND customer_name != ''")
@dlt.expect_or_drop("valid_customer_email", "customer_email IS NOT NULL AND customer_email LIKE '%@%'")
@dlt.expect_or_drop("valid_customer_phone", "customer_phone IS NOT NULL AND length(customer_phone) >= 7")
@dlt.expect_or_drop("valid_country", "country IS NOT NULL AND country != ''")
def customers_silver():
    return(
        dlt.read("customers_bronze")
    )

# Silver Layer: Branches Dimension (Validated and Cleansed)
# ---------------------------------------------------------
# This table refines the raw Branches data ingested in the Bronze layer and
# applies data quality checks to ensure the dimension table contains only
# valid and reliable records. Because the Branches dataset is static and was
# loaded using batch ingestion, this transformation uses `dlt.read()` rather
# than a streaming read.
#
# Key points:
# - Enforces validity of branch_id, branch_name, country, and region using
#   `expect_or_drop`, removing incomplete or corrupted records.
# - Produces a clean, standardized dimension table for downstream lookups and
#   joins with the Car Sales fact table in the Gold layer.
# - Ensures strong data integrity across the pipeline by preventing malformed
#   reference data from propagating to analytical layers.
#
# This curated Silver table forms a trusted Branches dimension, essential for
# enriching fact data and supporting accurate reporting and dashboards.
@dlt.table(
    name="branches_silver",
    comment="The Enriched Data of Branches with Valid Expectations and Data Quality Checks"
)
@dlt.expect_or_drop("valid_branch_id", "branch_id IS NOT NULL AND branch_id > 0")
@dlt.expect_or_drop("valid_branch_name", "branch_name IS NOT NULL AND branch_name != ''")
@dlt.expect_or_drop("valid_country", "country IS NOT NULL AND country != ''")
@dlt.expect_or_drop("valid_region", "region IS NOT NULL AND region != ''")
def branches_silver():
  return(
      dlt.read("branches_bronze")
  )

# Silver Layer: Car Sales Fact (Validated, Cleaned, and Dimension-Verified)
# ------------------------------------------------------------------------
# This table refines the raw Car Sales fact data by applying strict data
# quality rules and validating foreign key relationships with the Dealers,
# Customers, and Branches dimensions. The Car Sales dataset is ingested in
# streaming mode, so this transformation uses `dlt.read_stream()` to ensure
# continuous processing of incoming sales files.
#
# Key points:
# - Applies multiple `expect_or_drop` constraints to enforce essential data
#   quality rules (valid identifiers, sale dates, VIN structure, pricing,
#   manufacturer details, and payment attributes).
# - Performs inner joins with the cleaned Silver dimension tables, ensuring
#   only sales records with matching dealer, customer, and branch references
#   are included. This guarantees strong referential integrity.
# - Selects and returns only the required curated fields, producing a clean,
#   analytics-ready fact dataset without introducing ambiguity from dimension
#   attributes.
#
# The resulting Silver Car Sales table serves as the trusted foundation for
# downstream Gold-level aggregations, dashboarding, and analytical workloads.
@dlt.table(
    name="carsales_silver",
    comment="Validated and curated Car Sales data enriched with dimension lookups."
)
@dlt.expect_or_drop("valid_sale_id", "sale_id IS NOT NULL AND sale_id != ''")
@dlt.expect_or_drop("valid_sale_date", "sale_date IS NOT NULL")
@dlt.expect_or_drop("valid_model_name", "model_name IS NOT NULL AND model_name != ''")
@dlt.expect_or_drop("valid_type", "type IS NOT NULL AND type != ''")
@dlt.expect_or_drop("valid_fuel_type", "fuel_type IS NOT NULL AND fuel_type != ''")
@dlt.expect_or_drop("valid_transmission_type", "transmission_type IS NOT NULL AND transmission_type != ''")
@dlt.expect_or_drop("valid_vin", "vin IS NOT NULL AND length(vin) = 17")
@dlt.expect_or_drop("valid_price", "price IS NOT NULL AND price > 0")
@dlt.expect_or_drop("valid_payment_mode", "payment_mode IS NOT NULL AND payment_mode != ''")
def carsales_silver():

    # Read fact table (streaming)
    fact_df = dlt.read_stream("carsales_bronze").alias("f")

    # Read dimensions (used only to validate FK existence)
    dealers_df   = dlt.read("dealers_silver").select("dealer_id").alias("d")
    customers_df = dlt.read("customers_silver").select("customer_id").alias("c")
    branches_df  = dlt.read("branches_silver").select("branch_id").alias("b")

    # Join only to ensure valid FK references
    validated_df = (
        fact_df
        .join(dealers_df, "dealer_id", "inner")
        .join(customers_df, "customer_id", "inner")
        .join(branches_df, "branch_id", "inner")
    )

    # Select required fields
    return validated_df.select(
        "sale_id",
        "sale_date",
        "car_manufacturer",
        "model_name",
        "dealer_id",
        "customer_id",
        "branch_id",
        "price",
        "payment_mode",
        "type",
        "fuel_type",
        "transmission_type",
        "vin",
        "country",
        "region"
    )