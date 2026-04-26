# realtime-inventory-watch-system
<!-- Create topic clickstream-->
docker exec -it kafka kafka-topics --create --topic clickstream --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
<!-- Check created topic list -->
docker exec -it kafka-1 kafka-topics --list --bootstrap-server localhost:9092
<!-- Run Producer -->
python producer.py
