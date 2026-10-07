# Databricks notebook source

from pyspark.sql.functions import sum, col, when

# COMMAND ----------

storage_account = "azurecentralindiast"
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
    dbutils.secrets.get("databricksScope", "secretkv")
)

RAW = f"wasbs://raw-data@{storage_account}.blob.core.windows.net"
OUT = f"wasbs://transformed-data@{storage_account}.blob.core.windows.net"

# COMMAND ----------

def read_csv(path):
    return (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(path))

accounts_df = read_csv(f"{RAW}/accounts.csv")
data_dictionary_df = read_csv(f"{RAW}/data_dictionary.csv")
products_df = read_csv(f"{RAW}/products.csv")
sales_pipeline_df = read_csv(f"{RAW}/sales_pipeline.csv")
sales_teams_df = read_csv(f"{RAW}/sales_teams.csv")

# COMMAND ----------

accounts_df = accounts_df.withColumnRenamed("subsidiary_of", "parent_company")
data_dictionary_df = (data_dictionary_df
                      .withColumnRenamed("Table", "table")
                      .withColumnRenamed("Field", "field")
                      .withColumnRenamed("Description", "description"))

# COMMAND ----------

accounts_df = accounts_df.fillna({"parent_company": "Independent"})
sales_pipeline_df = sales_pipeline_df.fillna({"account": "unknown"})

# COMMAND ----------

def check_no_nulls(df, name, columns):
    nulls = df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns
    ]).collect()[0].asDict()
    bad = {k: v for k, v in nulls.items() if v > 0}
    if bad:
        raise ValueError(f"Data quality check failed for {name}: {bad}")
    print(f"{name}: data quality check passed")

check_no_nulls(accounts_df, "accounts", ["parent_company"])
check_no_nulls(sales_pipeline_df, "sales_pipeline", ["account"])

# COMMAND ----------

def write_csv(df, name):
    (df.write
       .mode("overwrite")
       .option("header", "true")
       .csv(f"{OUT}/{name}"))
    print(f"wrote {name}")

write_csv(accounts_df, "accounts")
write_csv(data_dictionary_df, "data_dictionary")
write_csv(products_df, "products")
write_csv(sales_pipeline_df, "sales_pipeline")
write_csv(sales_teams_df, "sales_teams")

print("Pipeline completed successfully")