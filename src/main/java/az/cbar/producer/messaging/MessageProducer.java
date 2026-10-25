package az.cbar.producer.messaging;

import az.cbar.producer.config.KafkaAppProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaAppProperties props;

    /**
     * Sends one message and waits for the broker acknowledgement (acks=all).
     * Records with the same key always land on the same partition, so their order is kept.
     * A null key spreads records across partitions.
     *
     * @throws MessageSendException when the broker did not acknowledge the write in time;
     *                              the caller decides whether to retry, alert or park the message
     */
    public RecordMetadata send(String key, String payload) {
        String topic = props.topic().name();
        try {
            RecordMetadata metadata = kafkaTemplate.send(topic, key, payload)
                    .get(props.sendTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getRecordMetadata();
            log.info("Sent key={} to {}-{} offset={}", key, metadata.topic(), metadata.partition(), metadata.offset());
            return metadata;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessageSendException(topic, key, e);
        } catch (ExecutionException | TimeoutException e) {
            // Never swallow this: an unacknowledged send is a possibly lost message
            log.error("Send failed key={} topic={}: {}", key, topic, e.getMessage());
            throw new MessageSendException(topic, key, e);
        }
    }
}
