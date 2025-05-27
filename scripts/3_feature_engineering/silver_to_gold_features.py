from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, year, round as pyspark_round, lit, when, count, isnan, isnull
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SilverToGoldProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("SilverToGoldFeatureEngineering") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Paths sesuai struktur HDFS yang sudah dibuat sebelumnya
        self.bps_path = "hdfs://namenode:9000/silver/bps_agriculture_data"
        self.bmkg_path = "hdfs://namenode:9000/silver/bmkg_weather_data"
        self.padi_csv_path = "hdfs://namenode:9000/silver/padi_features/Data_Tanaman_Padi_Sumatera.csv"
        self.gold_path = "hdfs://namenode:9000/gold/padi_features"
        
        logger.info("Spark session initialized for Silver to Gold processing")
    
    def load_silver_data(self):
        """Load data from silver layer"""
        logger.info("Loading data from silver layer...")
        
        try:
            # Load BPS data
            logger.info(f"Loading BPS data from: {self.bps_path}")
            bps_df = self.spark.read.parquet(self.bps_path)
            logger.info(f"BPS data loaded: {bps_df.count()} records")
            
            # Load BMKG data
            logger.info(f"Loading BMKG data from: {self.bmkg_path}")
            bmkg_df = self.spark.read.parquet(self.bmkg_path)
            logger.info(f"BMKG data loaded: {bmkg_df.count()} records")
            
            # Load Padi CSV data
            logger.info(f"Loading Padi CSV data from: {self.padi_csv_path}")
            padi_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("encoding", "UTF-8") \
                .csv(self.padi_csv_path)
            logger.info(f"Padi CSV data loaded: {padi_df.count()} records")
            
            # Show schemas for debugging
            logger.info("BPS Schema:")
            bps_df.printSchema()
            
            logger.info("BMKG Schema:")
            bmkg_df.printSchema()
            
            logger.info("Padi CSV Schema:")
            padi_df.printSchema()
            padi_df.show(5, truncate=False)
            
            return bps_df, bmkg_df, padi_df
            
        except Exception as e:
            logger.error(f"Error loading silver data: {str(e)}")
            raise
    
    def create_weather_features(self, bmkg_df):
        """Create aggregated weather features per province per year"""
        logger.info("Creating weather features...")
        
        # Extract year from date column
        if "tanggal" in bmkg_df.columns:
            bmkg_df = bmkg_df.withColumn("Tahun", year(col("tanggal")))
        else:
            logger.warning("Column 'tanggal' not found, using 2023 as default year")
            bmkg_df = bmkg_df.withColumn("Tahun", lit(2023))
        
        # Check available weather columns
        weather_cols = ["rainfall", "humidity_avg", "temp_avg", "temp_min", "temp_max"]
        available_cols = [col_name for col_name in weather_cols if col_name in bmkg_df.columns]
        
        logger.info(f"Available weather columns: {available_cols}")
        
        # Create aggregations based on available columns
        agg_exprs = []
        
        if "rainfall" in available_cols:
            agg_exprs.append(pyspark_round(avg("rainfall"), 2).alias("Curah_hujan"))
        
        if "humidity_avg" in available_cols:
            agg_exprs.append(pyspark_round(avg("humidity_avg"), 2).alias("Kelembapan"))
        
        if "temp_avg" in available_cols:
            agg_exprs.append(pyspark_round(avg("temp_avg"), 2).alias("Suhu_rata_rata"))
        elif "temp_min" in available_cols and "temp_max" in available_cols:
            # Calculate average temperature from min and max
            agg_exprs.append(pyspark_round(avg((col("temp_min") + col("temp_max")) / 2), 2).alias("Suhu_rata_rata"))
        
        if not agg_exprs:
            logger.error("No weather columns available for aggregation")
            raise ValueError("No weather columns found")
        
        # Aggregate weather data by province and year
        weather_agg = bmkg_df.groupBy("province", "Tahun").agg(*agg_exprs)
        
        logger.info(f"Weather aggregation completed: {weather_agg.count()} records")
        weather_agg.show(5, truncate=False)
        
        return weather_agg
    
    def prepare_integrated_data(self, bps_df, padi_df):
        """Integrate BPS and Padi CSV data"""
        logger.info("Integrating BPS and Padi CSV data...")
        
        # Show original columns
        logger.info(f"Original BPS columns: {bps_df.columns}")
        logger.info(f"Original Padi CSV columns: {padi_df.columns}")
        
        # If Padi CSV already contains integrated data, use it as primary source
        if "Provinsi" in padi_df.columns:
            logger.info("Using Padi CSV as primary data source")
            
            # Standardize column names if needed
            column_mapping = {
                "Provinsi": "province",
                "Tahun": "Tahun", 
                "Produksi": "Produksi",
                "Luas Panen": "Luas_Panen",
                "Curah hujan": "Curah_hujan",
                "Kelembapan": "Kelembapan",
                "Suhu rata-rata": "Suhu_rata_rata"
            }
            
            integrated_df = padi_df
            for old_col, new_col in column_mapping.items():
                if old_col in integrated_df.columns and old_col != new_col:
                    integrated_df = integrated_df.withColumnRenamed(old_col, new_col)
                    logger.info(f"Renamed column: {old_col} -> {new_col}")
            
        else:
            # Fallback: use BPS data and add year
            logger.info("Using BPS data as primary source")
            
            # Rename BPS columns
            bps_mapping = {
                "provinsi": "province",
                "produksi_ton": "Produksi",
                "luas_panen_ha": "Luas_Panen",
                "produktivitas_ku_ha": "Produktivitas"
            }
            
            integrated_df = bps_df
            for old_col, new_col in bps_mapping.items():
                if old_col in integrated_df.columns:
                    integrated_df = integrated_df.withColumnRenamed(old_col, new_col)
                    logger.info(f"Renamed column: {old_col} -> {new_col}")
            
            # Add year column
            if "Tahun" not in integrated_df.columns:
                integrated_df = integrated_df.withColumn("Tahun", lit(2023))
                logger.info("Added Tahun column with value 2023")
        
        logger.info(f"Integrated data columns: {integrated_df.columns}")
        logger.info(f"Integrated data count: {integrated_df.count()}")
        integrated_df.show(5, truncate=False)
        
        return integrated_df
    
    def create_gold_features(self, integrated_df, weather_agg):
        """Create final gold layer features"""
        logger.info("Creating gold layer features...")
        
        # If weather data is already integrated in CSV, use it directly
        if all(col in integrated_df.columns for col in ["Curah_hujan", "Kelembapan", "Suhu_rata_rata"]):
            logger.info("Weather data already integrated in CSV, using directly")
            gold_df = integrated_df
        else:
            # Join with weather aggregation
            logger.info("Joining with weather aggregation data")
            gold_df = integrated_df.join(weather_agg, on=["province", "Tahun"], how="left")
        
        logger.info(f"Gold features count: {gold_df.count()}")
        
        # Select and order final columns
        final_columns = []
        
        # Required columns
        required_cols = ["province", "Tahun", "Produksi", "Luas_Panen", "Curah_hujan", "Kelembapan", "Suhu_rata_rata"]
        
        for col_name in required_cols:
            if col_name in gold_df.columns:
                final_columns.append(col_name)
            else:
                logger.warning(f"Column {col_name} not found in data")
        
        # Select final columns and rename province for output
        if final_columns:
            gold_df = gold_df.select(*final_columns)
            
            # Rename province to Provinsi for final output
            if "province" in gold_df.columns:
                gold_df = gold_df.withColumnRenamed("province", "Provinsi")
            
            # Order by province for consistent output
            gold_df = gold_df.orderBy("Provinsi", "Tahun")
            
            logger.info("Gold features created successfully")
            gold_df.show(20, truncate=False)
        else:
            logger.error("No valid columns found for gold features")
            raise ValueError("No valid columns found")
        
        return gold_df
    
    def save_gold_data(self, gold_df):
        """Save gold layer data to HDFS"""
        logger.info(f"Saving gold data to: {self.gold_path}")
        
        try:
            # Create gold directory if not exists
            gold_df.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(self.gold_path)
            
            logger.info("Gold data saved successfully")
            
            # Verify saved data
            verification_df = self.spark.read.parquet(self.gold_path)
            logger.info(f"Verification: {verification_df.count()} records saved")
            
        except Exception as e:
            logger.error(f"Error saving gold data: {str(e)}")
            raise
    
    def export_to_local(self):
        """Export gold data to local dataset folder"""
        logger.info("Exporting gold data to local dataset folder...")
        
        try:
            # Load from HDFS
            gold_df = self.spark.read.parquet(self.gold_path)
            
            # Save to local dataset folder (will be mounted in Docker)
            local_path = "/dataset/gold/padi_features"
            
            gold_df.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(local_path)
            
            logger.info(f"Gold data exported to local path: {local_path}")
            
        except Exception as e:
            logger.warning(f"Could not export to local dataset folder: {str(e)}")
            logger.info("Please export manually using docker cp commands")
    
    def run_silver_to_gold_pipeline(self):
        """Run the complete silver to gold pipeline"""
        logger.info("Starting Silver to Gold pipeline")
        
        try:
            # Load silver data
            bps_df, bmkg_df, padi_df = self.load_silver_data()
            
            # Create weather features from BMKG
            weather_agg = self.create_weather_features(bmkg_df)
            
            # Prepare integrated data
            integrated_df = self.prepare_integrated_data(bps_df, padi_df)
            
            # Create gold features
            gold_df = self.create_gold_features(integrated_df, weather_agg)
            
            # Save to HDFS
            self.save_gold_data(gold_df)
            
            # Try to export to local (optional)
            self.export_to_local()
            
            logger.info("Silver to Gold pipeline completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
        
        finally:
            self.spark.stop()

def main():
    processor = SilverToGoldProcessor()
    success = processor.run_silver_to_gold_pipeline()
    
    if success:
        print("Silver to Gold processing completed successfully")
    else:
        print("Silver to Gold processing failed")

if __name__ == "__main__":
    main()