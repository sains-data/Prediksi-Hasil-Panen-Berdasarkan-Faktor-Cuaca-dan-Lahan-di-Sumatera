from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeToSilverProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("BronzeToSilver") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Paths
        self.bronze_path = "hdfs://namenode:9000/bronze"
        self.silver_path = "hdfs://namenode:9000/silver"
        
        logger.info("Spark session initialized for Bronze to Silver processing")
    
    def clean_bmkg_data(self, df, province):
        """Clean and standardize BMKG weather data sesuai contoh data"""
        logger.info(f"Cleaning BMKG data for province: {province}")

        # Rename columns to standardized names
        rename_dict = {
            "STASIUN": "stasiun",
            "TANGGAL": "tanggal",
            "TN": "temp_min",
            "TX": "temp_max",
            "TAVG": "temp_avg",
            "RH_AVG": "humidity_avg",
            "RR": "rainfall",
            "SS": "sunshine_duration",
            "FF_X": "wind_speed_max",
            "DDD_X": "wind_dir_max",
            "FF_AVG": "wind_speed_avg",
            "DDD_CAR": "wind_dir_avg"
        }
        for old_col, new_col in rename_dict.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)

        # Add province column
        df = df.withColumn("province", lit(province))

        # Standardize date column (format: dd/MM/yyyy)
        if "tanggal" in df.columns:
            df = df.withColumn("tanggal", to_date(col("tanggal"), "dd/MM/yyyy"))

        # Convert numeric columns
        numeric_cols = [
            "temp_min", "temp_max", "temp_avg", "humidity_avg",
            "rainfall", "sunshine_duration", "wind_speed_max",
            "wind_dir_max", "wind_speed_avg"
        ]
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, regexp_replace(col(col_name), "[^0-9.]", ""))
                df = df.withColumn(col_name, col(col_name).cast("double"))

        # Data quality: flag if rainfall or temp_avg is null
        df = df.withColumn(
            "data_quality_score",
            when(col("rainfall").isNull() | col("temp_avg").isNull(), 0.5).otherwise(1.0)
        )

        # Add processing timestamp
        df = df.withColumn("processed_at", current_timestamp())

        # Drop rows where all main numeric columns are null
        key_columns = [c for c in ["temp_min", "temp_max", "temp_avg", "humidity_avg", "rainfall"] if c in df.columns]
        if key_columns:
            df = df.dropna(how='all', subset=key_columns)

        return df
    
    def clean_bps_data(self, df):
        """Clean and standardize BPS agricultural data sesuai contoh data (gambar 2)"""
        logger.info("Cleaning BPS agricultural data")
    
        # Rename columns to standardized names
        rename_dict = {
            "Provinsi": "provinsi",
            "Luas Panen (ha)": "luas_panen_ha",
            "Produktivitas (ku/ha)": "produktivitas_ku_ha",
            "Produksi (ton)": "produksi_ton"
        }
        for old_col, new_col in rename_dict.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)
    
        # Standardize province names
        if "provinsi" in df.columns:
            df = df.withColumn("provinsi", upper(trim(col("provinsi"))))
    
        # Convert numeric columns
        numeric_cols = ["luas_panen_ha", "produktivitas_ku_ha", "produksi_ton"]
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, regexp_replace(col(col_name), "[^0-9.]", ""))
                df = df.withColumn(col_name, col(col_name).cast("double"))
    
        # Remove invalid records
        if "luas_panen_ha" in df.columns:
            df = df.filter(col("luas_panen_ha") > 0)
    
        # Add data quality score
        quality_conditions = [col(c).isNotNull() for c in numeric_cols if c in df.columns]
        if quality_conditions:
            df = df.withColumn(
                "data_quality_score",
                sum([when(cond, 1).otherwise(0) for cond in quality_conditions]) / len(quality_conditions)
            )
    
        # Add processing timestamp
        df = df.withColumn("processed_at", current_timestamp())
    
        return df
    
    def process_bmkg_files(self):
        """Process all BMKG files from bronze to silver"""
        logger.info("Processing BMKG files")
        
        provinces = ["aceh", "babel", "bengkulu", "jambi", "kepri", 
                    "lampung", "riau", "sumbar", "sumsel", "sumut"]
        
        all_bmkg_data = []
        
        for province in provinces:
            try:
                file_path = f"{self.bronze_path}/bmkg/{province}.csv"
                logger.info(f"Processing {file_path}")
                
                # Read CSV with proper schema inference
                df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("encoding", "UTF-8") \
                    .option("delimiter", ";") \
                    .csv(file_path)
    
                if df.count() > 0:
                    # Clean the data
                    cleaned_df = self.clean_bmkg_data(df, province)
                    all_bmkg_data.append(cleaned_df)
                    logger.info(f"Successfully processed {province}: {cleaned_df.count()} records")
                else:
                    logger.warning(f"No data found in {file_path}")
                    
            except Exception as e:
                logger.error(f"Error processing {province}: {str(e)}")
                continue
        
        if all_bmkg_data:
            # Union all dataframes
            combined_bmkg = all_bmkg_data[0]
            for df in all_bmkg_data[1:]:
                combined_bmkg = combined_bmkg.unionByName(df, allowMissingColumns=True)
            
            # Write to silver layer
            silver_bmkg_path = f"{self.silver_path}/bmkg_weather_data"
            combined_bmkg.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(silver_bmkg_path)
            
            logger.info(f"BMKG data written to silver layer: {combined_bmkg.count()} total records")
            return combined_bmkg
        else:
            logger.error("No BMKG data processed successfully")
            return None
    
    def process_bps_files(self):
        """Process BPS files from bronze to silver"""
        logger.info("Processing BPS files")
        
        try:
            file_path = f"{self.bronze_path}/bps/Luas_Panen_Produksi_dan_Produktivitas_Padi_2023.csv"
            logger.info(f"Processing {file_path}")
            
            # Read CSV
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("encoding", "UTF-8") \
                .option("delimiter", ";") \
                .csv(file_path)
            
            if df.count() > 0:
                # Clean the data
                cleaned_df = self.clean_bps_data(df)
                
                # Write to silver layer
                silver_bps_path = f"{self.silver_path}/bps_agriculture_data"
                cleaned_df.coalesce(1) \
                    .write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(silver_bps_path)
                
                logger.info(f"BPS data written to silver layer: {cleaned_df.count()} records")
                return cleaned_df
            else:
                logger.warning("No data found in BPS file")
                return None
                
        except Exception as e:
            logger.error(f"Error processing BPS data: {str(e)}")
            return None
    
    def validate_silver_data(self):
        """Validate the processed silver data"""
        logger.info("Validating silver layer data")
        
        try:
            # Validate BMKG data
            bmkg_path = f"{self.silver_path}/bmkg_weather_data"
            bmkg_df = self.spark.read.parquet(bmkg_path)
            
            bmkg_count = bmkg_df.count()
            bmkg_provinces = bmkg_df.select("province").distinct().count()
            
            logger.info(f"BMKG Silver Data Validation:")
            logger.info(f"  Total records: {bmkg_count}")
            logger.info(f"  Provinces: {bmkg_provinces}")
            logger.info(f"  Columns: {len(bmkg_df.columns)}")
            
            # Validate BPS data
            bps_path = f"{self.silver_path}/bps_agriculture_data"
            bps_df = self.spark.read.parquet(bps_path)
            
            bps_count = bps_df.count()
            
            logger.info(f"BPS Silver Data Validation:")
            logger.info(f"  Total records: {bps_count}")
            logger.info(f"  Columns: {len(bps_df.columns)}")
            
            # Data quality summary
            avg_quality_bmkg = bmkg_df.agg(avg("data_quality_score")).collect()[0][0]
            avg_quality_bps = bps_df.agg(avg("data_quality_score")).collect()[0][0]
            
            logger.info(f"Data Quality Scores:")
            logger.info(f"  BMKG Average Quality: {avg_quality_bmkg:.2f}")
            logger.info(f"  BPS Average Quality: {avg_quality_bps:.2f}")
            
            return True
            
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            return False
    
    def run_bronze_to_silver_pipeline(self):
        """Run the complete bronze to silver pipeline"""
        logger.info("Starting Bronze to Silver pipeline")
        
        try:
            # Process BMKG data
            bmkg_result = self.process_bmkg_files()
            
            # Process BPS data
            bps_result = self.process_bps_files()
            
            # Validate results
            if bmkg_result is not None and bps_result is not None:
                validation_success = self.validate_silver_data()
                
                if validation_success:
                    logger.info("Bronze to Silver pipeline completed successfully")
                    return True
                else:
                    logger.error("Data validation failed")
                    return False
            else:
                logger.error("Data processing failed")
                return False
                
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return False
        
        finally:
            self.spark.stop()

def main():
    processor = BronzeToSilverProcessor()
    success = processor.run_bronze_to_silver_pipeline()
    
    if success:
        print("Bronze to Silver processing completed successfully")
    else:
        print("Bronze to Silver processing failed")

if __name__ == "__main__":
    main()