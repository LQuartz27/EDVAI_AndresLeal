rm /home/hadoop/landing/starwars.csv

wget -P /home/hadoop/landing https://github.com/fpineyro/homework-0/blob/master/starwars.csv

/home/hadoop/hadoop/bin/hdfs dfs -rm /ingest/starwars.csv

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/starwars.csv /ingest/starwars.csv

rm /home/hadoop/landing/starwars.csv