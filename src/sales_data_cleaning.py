# Databricks notebook source

from pyspark.sql.functions import sum, col, when

# COMMAND ----------

def ensure_mount(container, mount_point):
    if not any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source=f"wasbs://{container}@azurecentralindiast.blob.core.windows.net",
            mount_point=mount_point,
            extra_configs={
                "fs.azure.account.key.azurecentralindiast.blob.core.windows.net":
                    dbutils.secrets.get("databricksScope", "secretkv")
            }
        )
    print(f"{mount_point} ready")

ensure_mount("raw-data", "/mnt/raw-data")
ensure_mount("transformed-data", "/mnt/transformed-data")

# COMMAND ----------

def read_csv(path):
    return (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(path))

accounts_df = read_csv("/mnt/raw-data/accounts.csv")
data_dictionary_df = read_csv("/mnt/raw-data/data_dictionary.csv")
products_df = read_csv("/mnt/raw-data/products.csv")
sales_pipeline_df = read_csv("/mnt/raw-data/sales_pipeline.csv")
sales_teams_df = read_csv("/mnt/raw-data/sales_teams.csv")

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
       .csv(f"/mnt/transformed-data/{name}"))
    print(f"wrote {name}")

write_csv(accounts_df, "accounts")
write_csv(data_dictionary_df, "data_dictionary")
write_csv(products_df, "products")
write_csv(sales_pipeline_df, "sales_pipeline")
write_csv(sales_teams_df, "sales_teams")

print("Pipeline completed successfully")