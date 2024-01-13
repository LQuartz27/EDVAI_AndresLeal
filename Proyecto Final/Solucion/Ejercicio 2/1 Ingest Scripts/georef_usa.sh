FILE=georef_usa.csv
FILEPATH=/home/hadoop/ingest/$FILE
URL="https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-united-states-of-america-state/exports/csv?lang=en&timezone=America%2FArgentina%2FBuenos_Aires&use_labels=true&delimiter=%3B"

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