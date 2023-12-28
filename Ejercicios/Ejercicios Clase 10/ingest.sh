FILE=/home/nifi/ingest/titanic.csv

if test -f "$FILE"; then
    echo "$FILE already exists"
    rm /home/nifi/ingest/titanic.csv
    echo "$FILE was removed"
else
    echo "$FILE didnt exist."
fi

wget -P /home/nifi/ingest https://raw.githubusercontent.com/fpineyro/homework-0/master/titanic.csv -o titanic.csv
echo "$FILE was downloaded"
