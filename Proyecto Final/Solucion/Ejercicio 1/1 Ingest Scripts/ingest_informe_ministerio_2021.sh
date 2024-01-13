FILE=2021_informe_ministerio.csv
FILEPATH=/home/hadoop/ingest/$FILE
URL="https://edvaibucket.blob.core.windows.net/data-engineer-edvai/2021-informe-ministerio.csv?sp=r&st=2023-11-06T12:59:46Z&se=2025-11-06T20:59:46Z&sv=2022-11-02&sr=b&sig=%2BSs5xIW3qcwmRh5TTmheIY9ZBa9BJC8XQDcI%2FPLRe9Y%3D"

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