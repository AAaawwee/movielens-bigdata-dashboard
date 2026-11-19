import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

public final class ReplayRatings {
    private static final String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    private static Properties connection(String bootstrap) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap);
        properties.setProperty("default.api.timeout.ms", "15000");
        return properties;
    }

    private static String lastValue(KafkaConsumer<String, String> consumer, TopicPartition partition, long offset) {
        consumer.seek(partition, offset);
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                if (record.offset() == offset) {
                    return record.value();
                }
            }
        }
        throw new IllegalStateException("Cannot read Kafka offset " + offset);
    }

    private static void checkSend(AtomicReference<Exception> failure) throws Exception {
        if (failure.get() != null) {
            throw new IllegalStateException("Kafka did not acknowledge a rating", failure.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Usage: ReplayRatings BOOTSTRAP TOPIC EXPECTED_ROWS");
        }
        String topic = args[1];
        long expected = Long.parseLong(args[2]);
        TopicPartition partition = new TopicPartition(topic, 0);
        Properties read = connection(args[0]);
        read.setProperty("key.deserializer", DESERIALIZER);
        read.setProperty("value.deserializer", DESERIALIZER);
        read.setProperty("enable.auto.commit", "false");
        Properties write = connection(args[0]);
        write.setProperty("key.serializer", SERIALIZER);
        write.setProperty("value.serializer", SERIALIZER);
        write.setProperty("acks", "all");
        write.setProperty("enable.idempotence", "true");
        write.setProperty("linger.ms", "15");
        write.setProperty("compression.type", "gzip");
        AtomicReference<Exception> failure = new AtomicReference<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(read);
             KafkaProducer<String, String> producer = new KafkaProducer<>(write);
             BufferedReader input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.US_ASCII))) {
            if (consumer.partitionsFor(topic).size() != 1) {
                throw new IllegalStateException("Replay expects exactly one Kafka partition");
            }
            consumer.assign(Collections.singleton(partition));
            long beginning = consumer.beginningOffsets(Collections.singleton(partition)).get(partition);
            long alreadySent = consumer.endOffsets(Collections.singleton(partition)).get(partition);
            if (beginning != 0 || alreadySent > expected) {
                throw new IllegalStateException("Topic offsets do not match this replay");
            }
            String lastExisting = alreadySent == 0 ? null : lastValue(consumer, partition, alreadySent - 1);
            long rows = 0;
            long lastTime = 0;
            String line;
            while ((line = input.readLine()) != null) {
                String[] fields = line.split("::", -1);
                if (fields.length != 4 || rows >= expected) {
                    throw new IllegalArgumentException("Malformed or unexpected rating at index " + rows);
                }
                int user = Integer.parseInt(fields[0]);
                int movie = Integer.parseInt(fields[1]);
                int rating = Integer.parseInt(fields[2]);
                long timestamp = Long.parseLong(fields[3]);
                if (user <= 0 || movie <= 0 || rating < 1 || rating > 5 || timestamp <= 0 || timestamp < lastTime) {
                    throw new IllegalArgumentException("Invalid or unordered rating at index " + rows);
                }
                lastTime = timestamp;
                String value = "{\"event_seq\":" + rows + ",\"user_id\":" + user + ",\"movie_id\":" + movie
                    + ",\"rating\":" + rating + ",\"timestamp\":" + timestamp + "}";
                if (rows == alreadySent - 1 && !value.equals(lastExisting)) {
                    throw new IllegalStateException("The existing topic is not a prefix of this MovieLens replay");
                }
                if (rows >= alreadySent) {
                    producer.send(new ProducerRecord<>(topic, 0, Integer.toString(user), value),
                        (metadata, error) -> { if (error != null) failure.compareAndSet(null, error); });
                }
                rows++;
                checkSend(failure);
                if (rows % 100000 == 0) {
                    System.out.println("REPLAY_PROGRESS prepared_rows=" + rows);
                }
            }
            producer.flush();
            checkSend(failure);
            long end = consumer.endOffsets(Collections.singleton(partition)).get(partition);
            if (rows != expected || end != expected) {
                throw new IllegalStateException("Count mismatch: source=" + rows + ", Kafka=" + end + ", expected=" + expected);
            }
            System.out.println("KAFKA_REPLAY_OK source_rows=" + rows + " partition=0 beginning=0 end=" + end
                + " newly_published=" + (end - alreadySent));
        }
    }
}
