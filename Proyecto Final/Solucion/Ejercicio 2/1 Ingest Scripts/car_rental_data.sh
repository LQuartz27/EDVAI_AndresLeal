FILE=CarRentalData.csv
FILEPATH=/home/hadoop/ingest/$FILE
URL="https://edvaibucket.blob.core.windows.net/data-engineer-edvai/CarRentalData.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D"

HDFS_FILEPATH="/ingest/$FILE"

if test -f "$FILEPATH"; then
    echo "$FILEPATH already exists"
    rm $FILEPATH
    echo "$FILEPATH was removed"
else
    echo "$FILEPATH didnt exist."
fi

wget -O "$FILEPATH" "$URL"
echo "$FILEPATH was downloaded"

# Remove the files if they exist in the HDFS ingestion folder
/home/hadoop/hadoop/bin/hdfs dfs -rm "$HDFS_FILEPATH"

# Putting the files in the HDFS ingestion folder
/home/hadoop/hadoop/bin/hdfs dfs -put "$FILEPATH" "$HDFS_FILEPATH"