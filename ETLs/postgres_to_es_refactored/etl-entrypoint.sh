#!/bin/sh

# дождемся готовности elasticsearch
es_status=0
while [ $es_status -ne 200 ]
do
es_status=$(curl --write-out %{http_code} --silent --output /dev/null http://es:9200/)
sleep 1
done

echo "try create es index"
python3 create_es_schemas.py


while true
do

echo "start genre_etl"
python3  postgres_to_es_refactored/genre_etl.py

echo "start person_etl"
python3  postgres_to_es_refactored/person_etl.py

echo "next launch in" $ETL_SLEEP_TIME
sleep $ETL_SLEEP_TIME

done
