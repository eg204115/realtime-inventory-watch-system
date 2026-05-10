# realtime-inventory-watch-system
<!-- Create topic clickstream-->
docker exec -it kafka kafka-topics --create --topic clickstream --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
<!-- Check created topic list -->
docker exec -it kafka-1 kafka-topics --list --bootstrap-server localhost:9092
<!-- verify events are sent to the topic -->
kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic clickstream-events \
--from-beginning
<!-- Run Producer -->
python producer.py

<!-- inside spark-local docker container -->
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--conf spark.jars.ivy=/tmp/.ivy \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
/opt/spark/jobs/streaming_job.py


spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 --conf spark.pyspark.python=python spark_stream.py

/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.3 \
/opt/spark/jobs/streaming_job.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 --conf spark.pyspark.python=python streaming_job.py

<!-- Run Dashboard -->
docker compose up -d dashboard
<!-- Open -->
http://localhost:8501

# schedule_interval="0 2 * * *",   runs daily at 2 AM