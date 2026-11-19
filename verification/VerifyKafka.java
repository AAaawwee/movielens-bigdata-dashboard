import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public final class VerifyKafka {
    public static void main(String[] args) throws Exception {
        String topic = "movie_rating_events";
        Properties config = new Properties();
        config.put("bootstrap.servers", args[0]);
        config.put("enable.auto.commit", "false");
        config.put("default.api.timeout.ms", "15000");
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());
        Pattern json = Pattern.compile("\\{\"event_seq\":(\\d+),\"user_id\":(\\d+),\"movie_id\":(\\d+),\"rating\":([1-5]),\"timestamp\":(\\d+)\\}");
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            if (consumer.partitionsFor(topic).size() != 1) {
                throw new IllegalStateException("Expected one topic partition");
            }
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(Collections.singletonList(partition));
            long beginning = consumer.beginningOffsets(Collections.singleton(partition)).get(partition);
            long end = consumer.endOffsets(Collections.singleton(partition)).get(partition);
            consumer.seek(partition, beginning);
            long offset = beginning;
            long previousTimestamp = 0;
            long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
            while (offset < end) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("Timed out at offset " + offset);
                }
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.offset() >= end) break;
                    Matcher fields = json.matcher(record.value());
                    if (!fields.matches() || record.offset() != offset || Long.parseLong(fields.group(1)) != offset) {
                        throw new IllegalStateException("Invalid message or missing offset at " + offset);
                    }
                    long timestamp = Long.parseLong(fields.group(5));
                    if (timestamp < previousTimestamp) throw new IllegalStateException("Disordered event at " + offset);
                    previousTimestamp = timestamp;
                    String line = String.join("::", fields.group(2), fields.group(3), fields.group(4), fields.group(5)) + "\n";
                    digest.update(line.getBytes(StandardCharsets.UTF_8));
                    offset++;
                }
            }
            StringBuilder checksum = new StringBuilder();
            for (byte b : digest.digest()) checksum.append(String.format("%02x", b & 0xff));
            System.out.printf("{\"topic\":\"%s\",\"beginning\":%d,\"end\":%d,\"count\":%d,\"sha256\":\"%s\"}%n",
                topic, beginning, end, offset - beginning, checksum);
        }
    }
}
