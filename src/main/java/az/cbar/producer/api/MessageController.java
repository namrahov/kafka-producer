package az.cbar.producer.api;

import az.cbar.producer.messaging.MessageProducer;
import az.cbar.producer.messaging.MessageSendException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessageProducer producer;

    @PostMapping
    public SendMessageResponse send(@Valid @RequestBody SendMessageRequest request) {
        RecordMetadata metadata = producer.send(request.key(), request.payload());
        return new SendMessageResponse(metadata.topic(), metadata.partition(), metadata.offset());
    }

    @ExceptionHandler(MessageSendException.class)
    public ProblemDetail sendFailed(MessageSendException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }

    public record SendMessageRequest(String key, @NotBlank String payload) {
    }

    public record SendMessageResponse(String topic, int partition, long offset) {
    }
}
