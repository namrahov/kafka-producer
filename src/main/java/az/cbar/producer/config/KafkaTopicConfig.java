package az.cbar.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topic in code. On startup Spring's KafkaAdmin creates it if missing;
 * an existing topic is left as is (partitions can only grow, never shrink).
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic messagesTopic(KafkaAppProperties props) {
        var topic = props.topic();
        return TopicBuilder.name(topic.name())
                .partitions(topic.partitions())
                .replicas(topic.replicas())
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(topic.minInsyncReplicas()))
                .build();
    }
}
