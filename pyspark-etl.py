"""
Databricks ETL Pipeline with Delta Lake
--------------------------------------
This script demonstrates a complete ETL pipeline using PySpark and Delta Lake.
The main steps include:
1. Setting up a Spark session with Delta Lake support.
2. Creating or retrieving a Delta table.
3. Transforming the data by adding a derived column.
4. Performing optimization and cleaning of the Delta table.
5. Merging new data into the Delta table using upsert operations.

The pipeline is designed to be modular and reusable for production-grade workflows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from delta.tables import DeltaTable

def create_spark_session():
    """Initialize a Spark session configured with Delta Lake support."""
    try:
        return SparkSession.builder \
            .appName("ETL_DeltaLake_Pipeline") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        raise

def create_or_get_delta_table(spark, table_path, df):
    """Create a Delta table if it doesn't exist or retrieve the existing table.

    Args:
        spark (SparkSession): Active Spark session.
        table_path (str): Path to the Delta table.
        df (DataFrame): DataFrame to write or merge into the Delta table.

    Returns:
        DeltaTable: The Delta table object.
    """
    try:
        if DeltaTable.isDeltaTable(spark, table_path):
            print(f"Delta table exists at {table_path}")
            return DeltaTable.forPath(spark, table_path)
        else:
            print(f"Creating new Delta table at {table_path}")
            df.write.format("delta").mode("overwrite").save(table_path)
            return DeltaTable.forPath(spark, table_path)
    except Exception as e:
        print(f"Error handling Delta table: {e}")
        raise

def transform_data(df):
    """Transform the data by adding an age group column.

    Args:
        df (DataFrame): Input DataFrame with an 'age' column.

    Returns:
        DataFrame: Transformed DataFrame with a new 'age_group' column.
    """
    try:
        return df.withColumn(
            "age_group",
            when(col("age") < 30, "young")
            .when((col("age") >= 30) & (col("age") < 40), "middle-aged")
            .otherwise("senior")
        )
    except Exception as e:
        print(f"Error during transformation: {e}")
        raise

def optimize_delta_table(delta_table):
    """Optimize the Delta table by compacting files and cleaning old versions.

    Args:
        delta_table (DeltaTable): Delta table object to optimize.
    """
    try:
        print("Optimizing Delta table...")
        delta_table.optimize().executeCompaction()
        print("Vacuuming old versions...")
        delta_table.vacuum(168)  # Retain the last 7 days of history
    except Exception as e:
        print(f"Error during optimization: {e}")
        raise

def main():
    """Main function to execute the ETL pipeline."""
    # Step 1: Initialize Spark session
    spark = create_spark_session()

    try:
        # Step 2: Define sample data
        data = [
            {"id": 1, "name": "Alice", "age": 28},
            {"id": 2, "name": "Bob", "age": 35},
            {"id": 3, "name": "Cathy", "age": 23},
            {"id": 4, "name": "David", "age": 40}
        ]

        # Step 3: Create a DataFrame
        df = spark.createDataFrame(data)

        # Step 4: Transform the data
        df_transformed = transform_data(df)

        # Step 5: Delta table path
        delta_table_path = "/mnt/delta/internal_etl_table"

        # Step 6: Create or retrieve Delta table
        delta_table = create_or_get_delta_table(spark, delta_table_path, df_transformed)

        # Step 7: Merge new data into the Delta table
        delta_table.alias("target").merge(
            df_transformed.alias("source"),
            "target.id = source.id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        # Step 8: Optimize the Delta table
        optimize_delta_table(delta_table)

        # Step 9: Show results
        print("Final Delta Table Contents:")
        delta_table.toDF().show()

        print("\nTable History:")
        delta_table.history().show()

    except Exception as e:
        print(f"Error in main execution: {e}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()

# Test Cases Section
# Add test cases to validate the behavior of each function
# Test 1: Validate Spark session creation
try:
    spark = create_spark_session()
    print("Spark session created successfully.")
except Exception as e:
    print(f"Test failed: {e}")

# Test 2: Validate data transformation
try:
    data = [{"id": 1, "age": 25}, {"id": 2, "age": 35}, {"id": 3, "age": 45}]
    df = spark.createDataFrame(data)
    df_transformed = transform_data(df)
    df_transformed.show()
except Exception as e:
    print(f"Test failed: {e}")
