import logging
import os
import time
from datetime import datetime
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.stat import Correlation
from pyspark.mllib.stat import Statistics

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import Python's built-in max function to avoid conflict with PySpark
from builtins import max as python_max
from builtins import min as python_min

class RiceProductionPredictor:
    """Rice Production Prediction using PySpark MLlib"""
    
    def __init__(self):
        """Initialize Spark session and paths"""
        self.spark = SparkSession.builder \
            .appName("RiceProductionMLPipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        # Paths
        self.gold_path = "hdfs://namenode:9000/gold/padi_features"
        self.model_path = "hdfs://namenode:9000/models/rice_production"
        self.synthetic_path = "hdfs://namenode:9000/gold/synthetic_rice_data"
        self.predictions_path = "hdfs://namenode:9000/predictions/rice_production"
        
        # Feature columns
        self.feature_cols = ["Luas_Panen", "Curah_hujan", "Kelembapan", "Suhu_rata_rata", "provinsi_indexed"]
        self.target_col = "Produksi"
        
        logger.info("Rice Production Predictor initialized")
        logger.info(f"Spark version: {self.spark.version}")
    
    def load_gold_data(self):
        """Load gold layer data"""
        logger.info("Loading gold layer data...")
        
        try:
            gold_df = self.spark.read.parquet(self.gold_path)
            logger.info(f"Gold data loaded: {gold_df.count()} records")
            
            # Show schema and sample
            logger.info("Gold data schema:")
            gold_df.printSchema()
            gold_df.show(5, truncate=False)
            
            return gold_df
            
        except Exception as e:
            logger.error(f"Failed to load gold data: {str(e)}")
            raise
    
    def generate_synthetic_data(self, gold_df, target_rows=1000000):
        """Generate synthetic data based on gold layer patterns"""
        logger.info(f"Generating {target_rows:,} synthetic records...")
        
        # Analyze existing data patterns
        stats = gold_df.describe().toPandas()
        provinces = [row.Provinsi for row in gold_df.select("Provinsi").distinct().collect()]
        
        logger.info(f"Base statistics from {gold_df.count()} real records:")
        logger.info(f"Provinces: {provinces}")
        
        # Extract statistics for synthetic generation
        def get_stat(col, stat):
            return float(stats[stats['summary'] == stat][col].iloc[0])
        
        # Define realistic ranges based on historical data
        # Using explicit python_max and python_min to avoid PySpark conflicts
        synthetic_ranges = {
            'Tahun': (1990, 2025),
            'Luas_Panen': (
                python_max(10000, get_stat('Luas_Panen', 'min') * 0.5),
                get_stat('Luas_Panen', 'max') * 1.5
            ),
            'Curah_hujan': (
                python_max(800, get_stat('Curah_hujan', 'min') * 0.8),
                python_min(3000, get_stat('Curah_hujan', 'max') * 1.2)
            ),
            'Kelembapan': (
                python_max(70, get_stat('Kelembapan', 'min') * 0.95),
                python_min(95, get_stat('Kelembapan', 'max') * 1.05)
            ),
            'Suhu_rata_rata': (
                python_max(24, get_stat('Suhu_rata_rata', 'min') * 0.95),
                python_min(32, get_stat('Suhu_rata_rata', 'max') * 1.05)
            )
        }
        
        logger.info("Synthetic data ranges:")
        for key, (min_val, max_val) in synthetic_ranges.items():
            logger.info(f"   {key}: [{min_val:.2f}, {max_val:.2f}]")
        
        # Generate synthetic data in batches to avoid memory issues
        batch_size = 100000
        batches = []
        
        logger.info(f"Generating data in batches of {batch_size:,}...")
        
        for batch_num in range(0, target_rows, batch_size):
            current_batch_size = python_min(batch_size, target_rows - batch_num)
            
            # Generate random data
            np.random.seed(42 + batch_num)  # Reproducible but varied
            
            synthetic_data = {
                'Provinsi': np.random.choice(provinces, current_batch_size),
                'Tahun': np.random.randint(
                    synthetic_ranges['Tahun'][0], 
                    synthetic_ranges['Tahun'][1], 
                    current_batch_size
                ),
                'Luas_Panen': np.random.uniform(
                    synthetic_ranges['Luas_Panen'][0],
                    synthetic_ranges['Luas_Panen'][1],
                    current_batch_size
                ).astype(int),
                'Curah_hujan': np.random.uniform(
                    synthetic_ranges['Curah_hujan'][0],
                    synthetic_ranges['Curah_hujan'][1],
                    current_batch_size
                ),
                'Kelembapan': np.random.uniform(
                    synthetic_ranges['Kelembapan'][0],
                    synthetic_ranges['Kelembapan'][1],
                    current_batch_size
                ),
                'Suhu_rata_rata': np.random.uniform(
                    synthetic_ranges['Suhu_rata_rata'][0],
                    synthetic_ranges['Suhu_rata_rata'][1],
                    current_batch_size
                )
            }
            
            # Calculate realistic production using complex formula
            # Based on agricultural research and historical correlations
            base_production = (
                synthetic_data['Luas_Panen'] * 4.5 +  # Base yield per hectare
                (synthetic_data['Curah_hujan'] - 1500) * 0.8 +  # Rainfall effect
                (85 - np.abs(synthetic_data['Kelembapan'] - 82)) * 50 +  # Optimal humidity
                (28 - np.abs(synthetic_data['Suhu_rata_rata'] - 27)) * 1000  # Optimal temperature
            )
            
            # Add province-specific multipliers
            province_multipliers = {
                'Aceh': 1.0, 'Sumatera Utara': 1.2, 'Sumatera Barat': 0.9,
                'Riau': 0.8, 'Jambi': 0.85, 'Sumatera Selatan': 1.1,
                'Bengkulu': 0.7, 'Lampung': 1.3
            }
            
            production_multiplier = np.array([
                province_multipliers.get(prov, 1.0) for prov in synthetic_data['Provinsi']
            ])
            
            # Add yearly trend (slight increase over time)
            year_trend = (synthetic_data['Tahun'] - 2000) * 500
            
            # Add realistic noise
            noise = np.random.normal(0, base_production * 0.1)
            
            synthetic_data['Produksi'] = np.maximum(
                1000,  # Minimum production
                (base_production * production_multiplier + year_trend + noise).astype(int)
            )
            
            # Create DataFrame for this batch
            batch_df = pd.DataFrame(synthetic_data)
            batches.append(batch_df)
            
            if (batch_num // batch_size + 1) % 5 == 0:
                logger.info(f"   Generated {batch_num + current_batch_size:,} records...")
        
        # Combine all batches
        logger.info("Combining batches...")
        synthetic_pandas_df = pd.concat(batches, ignore_index=True)
        
        # Convert to Spark DataFrame
        logger.info("Converting to Spark DataFrame...")
        schema = StructType([
            StructField("Provinsi", StringType(), True),
            StructField("Tahun", IntegerType(), True),
            StructField("Luas_Panen", LongType(), True),
            StructField("Curah_hujan", DoubleType(), True),
            StructField("Kelembapan", DoubleType(), True),
            StructField("Suhu_rata_rata", DoubleType(), True),
            StructField("Produksi", LongType(), True)
        ])
        
        synthetic_df = self.spark.createDataFrame(synthetic_pandas_df, schema=schema)
        
        # Combine with original gold data
        logger.info("Combining with original gold data...")
        combined_df = gold_df.union(synthetic_df)
        
        logger.info(f"Synthetic data generation completed:")
        logger.info(f"   Original records: {gold_df.count():,}")
        logger.info(f"   Synthetic records: {synthetic_df.count():,}")
        logger.info(f"   Total records: {combined_df.count():,}")
        
        # Save synthetic data
        logger.info("Saving synthetic dataset...")
        synthetic_df.coalesce(10) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(self.synthetic_path)
        
        # Show sample of synthetic data
        logger.info("Sample synthetic data:")
        synthetic_df.show(10, truncate=False)
        
        # Show statistics comparison
        logger.info("Statistics comparison:")
        logger.info("Original data:")
        gold_df.describe().show()
        logger.info("Synthetic data:")
        synthetic_df.describe().show()
        
        return combined_df
    
    def prepare_features(self, df):
        """Prepare features for machine learning"""
        logger.info("Preparing features for ML...")
        
        # Index categorical variables
        indexer = StringIndexer(inputCol="Provinsi", outputCol="provinsi_indexed")
        df = indexer.fit(df).transform(df)
        
        # Assemble features
        assembler = VectorAssembler(
            inputCols=self.feature_cols,
            outputCol="features"
        )
        df = assembler.transform(df)
        
        # Scale features
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        scaler_model = scaler.fit(df)
        df = scaler_model.transform(df)
        
        logger.info("Feature preparation completed")
        logger.info(f"Feature vector size: {len(self.feature_cols)}")
        
        return df, indexer, assembler, scaler_model
    
    def split_data(self, df, train_ratio=0.8):
        """Split data into train and test sets"""
        logger.info(f"Splitting data (train: {train_ratio}, test: {1-train_ratio})...")
        
        train_df, test_df = df.randomSplit([train_ratio, 1-train_ratio], seed=42)
        
        train_count = train_df.count()
        test_count = test_df.count()
        
        logger.info(f"Data split completed:")
        logger.info(f"   Training set: {train_count:,} records ({train_count/(train_count+test_count)*100:.1f}%)")
        logger.info(f"   Test set: {test_count:,} records ({test_count/(train_count+test_count)*100:.1f}%)")
        
        return train_df, test_df
    
    def train_models(self, train_df):
        """Train multiple ML models"""
        logger.info("Training machine learning models...")
        
        models = {}
        
        # 1. Random Forest Regressor
        logger.info("Training Random Forest...")
        rf = RandomForestRegressor(
            featuresCol="scaled_features",
            labelCol=self.target_col,
            numTrees=100,
            maxDepth=10,
            minInstancesPerNode=10,
            seed=42
        )
        
        rf_model = rf.fit(train_df)
        models['RandomForest'] = rf_model
        logger.info("Random Forest trained")
        
        # 2. Gradient Boosted Trees
        logger.info("Training Gradient Boosted Trees...")
        gbt = GBTRegressor(
            featuresCol="scaled_features",
            labelCol=self.target_col,
            maxIter=50,
            maxDepth=8,
            minInstancesPerNode=10,
            seed=42
        )
        
        gbt_model = gbt.fit(train_df)
        models['GBT'] = gbt_model
        logger.info("GBT trained")
        
        # 3. Linear Regression
        logger.info("Training Linear Regression...")
        lr = LinearRegression(
            featuresCol="scaled_features",
            labelCol=self.target_col,
            maxIter=100,
            regParam=0.1,
            elasticNetParam=0.1
        )
        
        lr_model = lr.fit(train_df)
        models['LinearRegression'] = lr_model
        logger.info("Linear Regression trained")
        
        return models
    
    def evaluate_models(self, models, test_df):
        """Evaluate all models"""
        logger.info("Evaluating models...")
        
        evaluator_rmse = RegressionEvaluator(
            labelCol=self.target_col,
            predictionCol="prediction",
            metricName="rmse"
        )
        
        evaluator_mae = RegressionEvaluator(
            labelCol=self.target_col,
            predictionCol="prediction",
            metricName="mae"
        )
        
        evaluator_r2 = RegressionEvaluator(
            labelCol=self.target_col,
            predictionCol="prediction",
            metricName="r2"
        )
        
        results = {}
        
        for name, model in models.items():
            logger.info(f"Evaluating {name}...")
            
            predictions = model.transform(test_df)
            
            rmse = evaluator_rmse.evaluate(predictions)
            mae = evaluator_mae.evaluate(predictions)
            r2 = evaluator_r2.evaluate(predictions)
            
            results[name] = {
                'RMSE': rmse,
                'MAE': mae,
                'R2': r2,
                'predictions': predictions
            }
            
            logger.info(f"   {name} Results:")
            logger.info(f"      RMSE: {rmse:,.2f}")
            logger.info(f"      MAE:  {mae:,.2f}")
            logger.info(f"      R²:   {r2:.4f}")
        
        # Find best model
        best_model_name = python_max(results.keys(), key=lambda k: results[k]['R2'])
        best_model = models[best_model_name]
        
        logger.info(f"Best model: {best_model_name} (R² = {results[best_model_name]['R2']:.4f})")
        
        return results, best_model_name, best_model
    
    def save_model(self, model, model_name):
        """Save the best model"""
        logger.info(f"Saving model: {model_name}")
        
        model_save_path = f"{self.model_path}/{model_name}"
        
        try:
            model.write().overwrite().save(model_save_path)
            logger.info(f"Model saved to: {model_save_path}")
        except Exception as e:
            logger.error(f"Failed to save model: {str(e)}")
    
    def generate_predictions(self, model, test_df, model_name):
        """Generate and save predictions"""
        logger.info("Generating predictions...")
        
        predictions = model.transform(test_df)
        
        # Select relevant columns
        prediction_df = predictions.select(
            "Provinsi", "Tahun", "Luas_Panen", "Curah_hujan", 
            "Kelembapan", "Suhu_rata_rata", 
            col(self.target_col).alias("Actual_Produksi"),
            col("prediction").alias("Predicted_Produksi")
        )
        
        # Add prediction metadata
        prediction_df = prediction_df.withColumn("Model", lit(model_name)) \
                                   .withColumn("Prediction_Date", lit(datetime.now().isoformat())) \
                                   .withColumn("Error", col("Predicted_Produksi") - col("Actual_Produksi")) \
                                   .withColumn("Error_Percentage", 
                                             (abs(col("Error")) / col("Actual_Produksi")) * 100)
        
        # Save predictions
        prediction_save_path = f"{self.predictions_path}/{model_name}"
        
        prediction_df.coalesce(5) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(prediction_save_path)
        
        logger.info(f"Predictions saved to: {prediction_save_path}")
        
        # Show sample predictions
        logger.info("Sample predictions:")
        prediction_df.show(10, truncate=False)
        
        # Prediction statistics
        logger.info("Prediction statistics:")
        prediction_df.describe(["Actual_Produksi", "Predicted_Produksi", "Error", "Error_Percentage"]).show()
        
        return prediction_df
    
    def analyze_feature_importance(self, model, model_name):
        """Analyze feature importance (for tree-based models)"""
        if hasattr(model, 'featureImportances'):
            logger.info(f"Analyzing feature importance for {model_name}...")
            
            importances = model.featureImportances.toArray()
            feature_importance = list(zip(self.feature_cols, importances))
            feature_importance.sort(key=lambda x: x[1], reverse=True)
            
            logger.info("Feature Importance:")
            for feature, importance in feature_importance:
                logger.info(f"   {feature}: {importance:.4f}")
        else:
            logger.info(f"Feature importance not available for {model_name}")
    
    def run_complete_pipeline(self):
        """Run the complete ML pipeline"""
        start_time = time.time()
        logger.info("Starting complete ML pipeline...")
        
        try:
            # 1. Load gold data
            gold_df = self.load_gold_data()
            
            # 2. Generate synthetic data
            combined_df = self.generate_synthetic_data(gold_df, target_rows=1000000)
            
            # 3. Prepare features
            prepared_df, indexer, assembler, scaler = self.prepare_features(combined_df)
            
            # 4. Split data
            train_df, test_df = self.split_data(prepared_df)
            
            # 5. Train models
            models = self.train_models(train_df)
            
            # 6. Evaluate models
            results, best_model_name, best_model = self.evaluate_models(models, test_df)
            
            # 7. Save best model
            self.save_model(best_model, best_model_name)
            
            # 8. Generate predictions
            predictions = self.generate_predictions(best_model, test_df, best_model_name)
            
            # 9. Analyze feature importance
            self.analyze_feature_importance(best_model, best_model_name)
            
            # Final summary
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info("ML Pipeline completed successfully!")
            logger.info(f"Total duration: {duration/60:.2f} minutes")
            logger.info(f"Best model: {best_model_name}")
            logger.info(f"Final R² score: {results[best_model_name]['R2']:.4f}")
            logger.info(f"Total training data: {train_df.count():,} records")
            logger.info(f"Test predictions: {test_df.count():,} records")
            
            return {
                'models': models,
                'best_model': best_model,
                'best_model_name': best_model_name,
                'results': results,
                'predictions': predictions,
                'preprocessing': {
                    'indexer': indexer,
                    'assembler': assembler,
                    'scaler': scaler
                }
            }
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        
        finally:
            logger.info("Closing Spark session...")
            self.spark.stop()

def main():
    """Main execution function"""
    logger.info("Rice Production Prediction Pipeline Starting...")
    
    predictor = RiceProductionPredictor()
    results = predictor.run_complete_pipeline()
    
    logger.info("All processes completed successfully!")
    return results

if __name__ == "__main__":
    main()