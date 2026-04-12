@echo off
:: Prints first 10 records from output topics: id, name, continent

set KAFKA_CONTAINER=kafka

:: Verify container is running
docker exec %KAFKA_CONTAINER% echo ok >nul 2>&1
if errorlevel 1 (
    echo Kafka container not found. Run: docker-compose up
    exit /b 1
)

echo Kafka is running (container: %KAFKA_CONTAINER%)
echo ----------------------------------------

echo.
echo Top 10 from id:
docker exec %KAFKA_CONTAINER% /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --max-messages 10 --topic id

echo.
echo Top 10 from name:
docker exec %KAFKA_CONTAINER% /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --max-messages 10 --topic name

echo.
echo Top 10 from continent:
docker exec %KAFKA_CONTAINER% /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --max-messages 10 --topic continent

echo.
echo Verification complete