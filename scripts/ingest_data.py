import os
from pyspark.sql import SparkSession

DATASET_DIR = "/bronze_local"
HDFS_TARGET_DIR = "hdfs://namenode:9000/bronze"

def main():
    spark = SparkSession.builder.appName("IngestDataBronze").getOrCreate()

    for root, dirs, files in os.walk(DATASET_DIR):
        for file in files:
            if file.endswith(".csv"):
                local_file = os.path.join(root, file)
                rel_dir = os.path.relpath(root, DATASET_DIR)
                hdfs_dir = os.path.join(HDFS_TARGET_DIR, rel_dir).replace("\\", "/")
                hdfs_path = f"{hdfs_dir}/{file.replace('.csv', '.parquet')}"
                print(f"Ingesting {local_file} to {hdfs_path} ...")
                df = spark.read.option("header", True).csv(local_file)
                df.write.mode("overwrite").parquet(hdfs_path)

    spark.stop()

if __name__ == "__main__":
    main()