from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
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
        
        # Province mapping untuk standardisasi nama
        self.province_mapping = {
            "aceh": "ACEH",
            "babel": "KEPULAUAN BANGKA BELITUNG",
            "bengkulu": "BENGKULU", 
            "jambi": "JAMBI",
            "kepri": "KEPULAUAN RIAU",
            "lampung": "LAMPUNG",
            "riau": "RIAU",
            "sumbar": "SUMATERA BARAT",
            "sumsel": "SUMATERA SELATAN",
            "sumut": "SUMATERA UTARA"
        }
        
        logger.info("Spark session initialized for Bronze to Silver processing")
    
    def interpolate_missing_values(self, df):
        """Interpolate missing values using mean for numeric columns"""
        logger.info("Interpolating missing values with mean values")
        
        # Numeric columns to interpolate
        numeric_cols = [
            "temp_min", "temp_max", "temp_avg", "humidity_avg",
            "rainfall", "sunshine_duration", "wind_speed_max",
            "wind_dir_max", "wind_speed_avg"
        ]
        
        for col_name in numeric_cols:
            if col_name in df.columns:
                # Calculate mean for the column (excluding nulls)
                mean_value = df.agg(avg(col(col_name))).collect()[0][0]
                
                if mean_value is not None:
                    # Fill null values with mean
                    df = df.withColumn(
                        col_name,
                        when(col(col_name).isNull(), lit(mean_value)).otherwise(col(col_name))
                    )
                    logger.info(f"Interpolated {col_name} missing values with mean: {mean_value:.2f}")
                else:
                    logger.warning(f"Could not calculate mean for {col_name} - all values are null")
        
        return df
    
    def clean_bmkg_data(self, df, province_code):
        """Clean and standardize BMKG weather data sesuai contoh data"""
        # Get standardized province name
        province_name = self.province_mapping.get(province_code, province_code.upper())
        logger.info(f"Cleaning BMKG data for province: {province_code} -> {province_name}")

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

        # Add province column dengan nama yang sudah distandarisasi
        df = df.withColumn("province", lit(province_name))
        df = df.withColumn("province_code", lit(province_code))

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
                # Clean non-numeric characters and convert to double
                df = df.withColumn(col_name, regexp_replace(col(col_name), "[^0-9.-]", ""))
                df = df.withColumn(col_name, 
                    when(col(col_name) == "", None)
                    .otherwise(col(col_name).cast("double"))
                )

        # Interpolate missing values
        df = self.interpolate_missing_values(df)

        # Data quality: calculate score based on completeness after interpolation
        df = df.withColumn(
            "data_quality_score",
            when(col("rainfall").isNull() | col("temp_avg").isNull(), 0.5).otherwise(1.0)
        )

        # Add processing timestamp
        df = df.withColumn("processed_at", current_timestamp())

        # Drop rows where all main numeric columns are still null after interpolation
        key_columns = [c for c in ["temp_min", "temp_max", "temp_avg", "humidity_avg", "rainfall"] if c in df.columns]
        if key_columns:
            # Only drop if ALL key columns are null
            null_condition = col(key_columns[0]).isNull()
            for col_name in key_columns[1:]:
                null_condition = null_condition & col(col_name).isNull()
            df = df.filter(~null_condition)

        logger.info(f"Processed {province_name}: interpolated missing values and cleaned data")
        return df
    
    def clean_bps_data(self, df):
        """Clean and standardize BPS agricultural data sesuai contoh data (gambar 2)"""
        logger.info("Cleaning BPS agricultural data")
    
        # Debug: Print original columns
        logger.info(f"Original BPS columns: {df.columns}")
        
        # Rename columns to standardized names - handle exact column names from CSV
        rename_dict = {
            "Provinsi": "provinsi",
            "Luas Panen (ha)": "luas_panen_ha",
            "Produktivitas (ku/ha)": "produktivitas_ku_ha",
            "Produksi (ton)": "produksi_ton"
        }
        
        # Check and rename existing columns
        for old_col, new_col in rename_dict.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)
                logger.info(f"Renamed column: {old_col} -> {new_col}")
    
        # Debug: Print columns after rename
        logger.info(f"Columns after rename: {df.columns}")
        
        # Standardize province names - keep original BPS format (uppercase)
        if "provinsi" in df.columns:
            df = df.withColumn("provinsi", upper(trim(col("provinsi"))))
    
        # Convert numeric columns - check if they exist after rename
        numeric_cols = ["luas_panen_ha", "produktivitas_ku_ha", "produksi_ton"]
        existing_numeric_cols = [c for c in numeric_cols if c in df.columns]
        
        logger.info(f"Processing numeric columns: {existing_numeric_cols}")
        
        for col_name in existing_numeric_cols:
            # Handle Indonesian number format (comma as decimal separator)
            df = df.withColumn(col_name, regexp_replace(col(col_name), "[^0-9.,]", ""))
            df = df.withColumn(col_name, regexp_replace(col(col_name), ",", "."))
            df = df.withColumn(col_name, 
                when(col(col_name) == "", None)
                .otherwise(col(col_name).cast("double"))
            )
    
        # Remove invalid records - only if luas_panen_ha exists and is positive
        if "luas_panen_ha" in df.columns:
            df = df.filter((col("luas_panen_ha") > 0) | col("luas_panen_ha").isNull())
    
        # Add data quality score - FIXED VERSION
        if existing_numeric_cols:
            # Calculate quality score by summing individual column conditions
            quality_expr = lit(0)
            for col_name in existing_numeric_cols:
                quality_expr = quality_expr + when(col(col_name).isNotNull() & (col(col_name) > 0), 1).otherwise(0)
            
            df = df.withColumn(
                "data_quality_score",
                (quality_expr / lit(len(existing_numeric_cols))).cast("double")
            )
        else:
            # Fallback if no numeric columns found
            df = df.withColumn("data_quality_score", lit(1.0))
    
        # Add processing timestamp
        df = df.withColumn("processed_at", current_timestamp())
        
        # Debug: Print final schema
        logger.info("Final BPS schema:")
        df.printSchema()
    
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
            
            # Show province standardization results
            logger.info("Province standardization mapping:")
            combined_bmkg.select("province_code", "province").distinct().orderBy("province_code").show(truncate=False)
            
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
            
            # Read CSV with different delimiter options
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("encoding", "UTF-8") \
                .option("delimiter", ",") \
                .csv(file_path)
            
            # If comma delimiter doesn't work, try semicolon
            if df.count() == 0 or len(df.columns) == 1:
                logger.info("Trying semicolon delimiter...")
                df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("encoding", "UTF-8") \
                    .option("delimiter", ";") \
                    .csv(file_path)
            
            logger.info(f"BPS file read successfully: {df.count()} records, {len(df.columns)} columns")
            logger.info(f"BPS columns: {df.columns}")
            
            if df.count() > 0:
                # Clean the data
                cleaned_df = self.clean_bps_data(df)
                
                # Show BPS province names for reference
                logger.info("BPS province names:")
                if "provinsi" in cleaned_df.columns:
                    cleaned_df.select("provinsi").distinct().orderBy("provinsi").show(truncate=False)
                
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
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
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
            
            # Check for missing values after interpolation
            logger.info("BMKG Missing values check after interpolation:")
            for col_name in ["temp_min", "temp_max", "temp_avg", "humidity_avg", "rainfall"]:
                if col_name in bmkg_df.columns:
                    null_count = bmkg_df.filter(col(col_name).isNull()).count()
                    logger.info(f"  {col_name}: {null_count} missing values")
            
            # Validate BPS data
            bps_path = f"{self.silver_path}/bps_agriculture_data"
            bps_df = self.spark.read.parquet(bps_path)
            
            bps_count = bps_df.count()
            
            logger.info(f"BPS Silver Data Validation:")
            logger.info(f"  Total records: {bps_count}")
            logger.info(f"  Columns: {len(bps_df.columns)}")
            
            # Data quality summary - check if data_quality_score exists
            bmkg_quality_avg = None
            bps_quality_avg = None
            
            if "data_quality_score" in bmkg_df.columns:
                bmkg_quality_avg = bmkg_df.agg(avg("data_quality_score")).collect()[0][0]
            
            if "data_quality_score" in bps_df.columns:
                bps_quality_avg = bps_df.agg(avg("data_quality_score")).collect()[0][0]
            
            logger.info(f"Data Quality Scores:")
            if bmkg_quality_avg is not None:
                logger.info(f"  BMKG Average Quality: {bmkg_quality_avg:.2f}")
            else:
                logger.info(f"  BMKG Average Quality: N/A (column not found)")
                
            if bps_quality_avg is not None:
                logger.info(f"  BPS Average Quality: {bps_quality_avg:.2f}")
            else:
                logger.info(f"  BPS Average Quality: N/A (column not found)")
            
            # Show sample data for debugging
            logger.info("BMKG Sample Data:")
            bmkg_df.select("province", "province_code", "tanggal", "temp_avg", "rainfall", "data_quality_score").show(5, truncate=False)
            
            logger.info("BPS Sample Data:")
            bps_df.show(5, truncate=False)
            
            # Province name comparison
            logger.info("Province name comparison:")
            logger.info("BMKG provinces:")
            bmkg_df.select("province").distinct().orderBy("province").show(truncate=False)
            logger.info("BPS provinces:")
            bps_df.select("provinsi").distinct().orderBy("provinsi").show(truncate=False)
            
            return True
            
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
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
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
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