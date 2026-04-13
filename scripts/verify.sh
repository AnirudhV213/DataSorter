#!/bin/bash
# Prints first 10 records from output topics: id, name, continent

KAFKA_CONTAINER=kafka

# Verify container is running
if ! docker exec "$KAFKA_CONTAINER" echo ok > /dev/null 2>&1; then
    echo " Kafka container not found. Run: docker-compose up"
    exit 1
fi

echo " Kafka is running (container: $KAFKA_CONTAINER)"
echo "----------------------------------------"

consume_topic () {
    local topic=$1
    echo ""
    echo "Top 10 from $topic:"

    docker exec -i "$KAFKA_CONTAINER" bash -c "
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic $topic \
    --from-beginning \
    --max-messages 10
    "
}

consume_topic "id"
consume_topic "name"
consume_topic "continent"

echo ""
echo " Verification complete"