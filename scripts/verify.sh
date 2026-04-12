#!/bin/bash
# Prints first 10 records from output topics: id, name, continent

KAFKA_CONTAINER=kafka

# Verify container is running
if ! docker exec "$KAFKA_CONTAINER" echo ok > /dev/null 2>&1; then
    echo "Kafka container not found. Run: docker-compose up"
    exit 1
fi

echo "Kafka is running (container: $KAFKA_CONTAINER)"
echo "----------------------------------------"

echo ""
echo "Top 10 from id:"
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --max-messages 10 --topic id

echo ""
echo "Top 10 from name:"
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --max-messages 10 --topic name

echo ""
echo "Top 10 from continent:"
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --max-messages 10 --topic continent

echo ""
echo "Verification complete"