package az.cbar.producer.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaAppProperties(@NotNull Topic topic, @NotNull Duration sendTimeout) {

    public record Topic(@NotBlank String name,
                        @Min(1) int partitions,
                        @Min(1) int replicas,
                        @Min(1) int minInsyncReplicas) {
    }
}
