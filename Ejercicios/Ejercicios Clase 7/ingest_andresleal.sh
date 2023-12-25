# Defining the landing filepaths
TRIP_DATA_FILE_1="/home/hadoop/landing/yellow_trip_data_2021-01.parquet"
TRIP_DATA_FILE_2="/home/hadoop/landing/yellow_trip_data_2021-02.parquet"

HDFS_FILEPATH_1="/ingest/yellow_trip_data_2021-01.parquet"
HDFS_FILEPATH_2="/ingest/yellow_trip_data_2021-02.parquet"

# Remove the files if they exist in the landing folder
if test -f "$TRIP_DATA_FILE_1"
then
    rm "$TRIP_DATA_FILE_1"
    echo -e "\n$TRIP_DATA_FILE_1 was removed. The latest version will be downloaded next.\n"
else
    echo -e "\n$TRIP_DATA_FILE_1 does not exist. The latest version will be downloaded next.\n"
fi

if test -f "$TRIP_DATA_FILE_2"
then
    rm "$TRIP_DATA_FILE_2"
    echo -e "\n$TRIP_DATA_FILE_2 was removed. The latest version will be downloaded next.\n"
else
    echo -e "\n$TRIP_DATA_FILE_2 does not exist. The latest version will be downloaded next.\n"

fi

#
# THIS CONDITIONAL IS FAILING WHEN AIRFLOW RUNS THE SCRIPT. IT LOOKS LIKE FROM INSIDE AIRFLOW ! command -v wget &> /dev/null 
# ALWAYS THROWS FALSE, EVEN WHEN wget EXISTS

#if ! command -v wget &> /dev/null
#then
#    echo "wget could not be found, please install wget."
#    exit 1
#else
wget -O "$TRIP_DATA_FILE_1" https://edvaibucket.blob.core.windows.net/data-engineer-edvai/yellow_tripdata_2021-01.parquet\?sp\=r\&st\=2023-11-06T12:52:39Z\&se\=2025-11-06T20:52:39Z\&sv\=2022-11-02\&sr\=c\&sig\=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D \
&& echo -e "\n$TRIP_DATA_FILE_1 was downloaded. \n"

#wget -O "$TRIP_DATA_FILE_2" https://edvaibucket.blob.core.windows.net/data-engineer-edvai/yellow_tripdata_2021-02.csv\?sp\=r\&st\=2023-11-06T12:52:39Z\&se\=2025-11-06T20:52:39Z\&sv\=2022-11-02\&sr\=c\&sig\=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D \
wget -O "$TRIP_DATA_FILE_2" https://edvaibucket.blob.core.windows.net/data-engineer-edvai/yellow_tripdata_2021-02.parquet\?sp\=r\&st\=2023-11-06T12:52:39Z\&se\=2025-11-06T20:52:39Z\&sv\=2022-11-02\&sr\=c\&sig\=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D \
&& echo -e "\n$TRIP_DATA_FILE_2 was downloaded.\n"
#fi

# Remove the files if they exist in the HDFS ingestion folder
/home/hadoop/hadoop/bin/hdfs dfs -rm "$HDFS_FILEPATH_1"
/home/hadoop/hadoop/bin/hdfs dfs -rm "$HDFS_FILEPATH_2"

# Putting the files in the HDFS ingestion folder
/home/hadoop/hadoop/bin/hdfs dfs -put "$TRIP_DATA_FILE_1" "$HDFS_FILEPATH_1"
/home/hadoop/hadoop/bin/hdfs dfs -put "$TRIP_DATA_FILE_2" "$HDFS_FILEPATH_2"

# Removing the files from the landing folder
rm "$TRIP_DATA_FILE_1"
rm "$TRIP_DATA_FILE_2"