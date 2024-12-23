
# Databricks ETL Pipeline with Delta Lake

## Overview

This project demonstrates an ETL pipeline for processing and managing data using PySpark and Delta Lake in a Databricks environment. It includes the following steps:

1. Initializing a Spark session with Delta Lake support.
2. Loading and transforming data.
3. Creating or retrieving a Delta Lake table.
4. Performing upserts (merge operations) to update or insert data.
5. Optimizing the Delta table by compacting files and cleaning up older versions.

## Features

- **Delta Lake Support**: Handles ACID transactions and data versioning.
- **Data Transformation**: Adds derived columns for analytical use.
- **Delta Table Optimization**: Compacts files and vacuums outdated versions for performance.

## Getting Started

### Prerequisites

- A Databricks workspace with an active cluster.
- Permissions to read and write to Delta tables in Databricks File System (DBFS).
- Sample or production data.

### Dependencies

This project assumes that the necessary libraries (`pyspark`, `delta-spark`) are pre-installed in the Databricks cluster. You can also install additional libraries using the Databricks `%pip install` command if needed.

## How to Run

### Set Up Cluster:
- Launch a cluster in Databricks.
- Ensure the runtime version supports Delta Lake and PySpark.

### Load the Script:
- Create a new Databricks notebook and copy the script from this repository into the notebook cells.

### Configure Paths:
- Update the `delta_table_path` variable in the script to point to your desired Delta table location on DBFS (e.g., `/mnt/delta/internal_etl_table`).

### Execute the Notebook:
- Run the cells in sequence. The script will:
  - Create or retrieve a Delta table.
  - Apply transformations to the data.
  - Perform upsert operations to merge new data.
  - Optimize and clean up the Delta table.

### Verify Results:
- Inspect the Delta table contents and history using the script's output.

## Directory Structure

This project is structured for easy navigation:

```
.
├── etl_pipeline.py   # Main ETL script
└── README.md         # Project documentation
```

## Code Highlights

### Data Transformation: Adds a derived column `age_group` based on age:
```python
df.withColumn(
    "age_group",
    when(col("age") < 30, "young")
    .when((col("age") >= 30) & (col("age") < 40), "middle-aged")
    .otherwise("senior")
)
```

### Delta Table Management: Creates or retrieves a Delta table:
```python
DeltaTable.forPath(spark, table_path)
```

### Upsert Operations: Merges new data into the existing Delta table:
```python
delta_table.alias("target").merge(
    df_transformed.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

## Best Practices

- **Optimize Delta Tables Regularly**: Compact small files and vacuum old versions to improve query performance.
- **Use Databricks Jobs**: Schedule this pipeline for periodic execution to automate ETL tasks.
- **Monitor Delta Table History**: Keep track of changes and recover previous versions if needed.

## Limitations

- This script is designed for Databricks and may not run locally without modifications.
- Ensure sufficient storage in the DBFS path for large datasets.
