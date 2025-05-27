DATASET_DIR="/tmp/bronze"
HDFS_DIR="/bronze"

echo "Starting data ingestion..."
echo "Using dataset directory: $DATASET_DIR"

# Verify dataset structure
if [ ! -d "$DATASET_DIR/bmkg" ] || [ ! -d "$DATASET_DIR/bps" ]; then
    echo "Error: Expected subdirectories 'bmkg' and 'bps' not found in $DATASET_DIR"
    echo "Current directory structure:"
    ls -la "$DATASET_DIR"
    exit 1
fi

# Create HDFS directories
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p $HDFS_DIR/bmkg $HDFS_DIR/bps

# Count and process files
total=0
success=0

echo "Processing CSV files..."
for csv_file in $(find $DATASET_DIR -name "*.csv"); do
    total=$((total + 1))
    filename=$(basename "$csv_file")
    
    # Get relative path for HDFS
    rel_path=${csv_file#$DATASET_DIR/}
    hdfs_path="$HDFS_DIR/$rel_path"
    
    echo "Processing: $filename"
    echo "  From: $csv_file"
    echo "  To: $hdfs_path"
    
    # Upload to HDFS
    if hdfs dfs -put -f "$csv_file" "$hdfs_path" 2>/dev/null; then
        echo "  SUCCESS: Uploaded to HDFS"
        success=$((success + 1))
    else
        echo "  ERROR: Failed to upload"
    fi
done

# Summary
echo ""
echo "SUMMARY:"
echo "  Dataset directory used: $DATASET_DIR"
echo "  Total files: $total"
echo "  Successful: $success"
echo "  Failed: $((total - success))"

# List HDFS contents
if [ $success -gt 0 ]; then
    echo ""
    echo "HDFS Contents:"
    hdfs dfs -ls -R $HDFS_DIR
fi

echo "Data ingestion completed"