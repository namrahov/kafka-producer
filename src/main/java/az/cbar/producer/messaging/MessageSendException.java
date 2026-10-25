package az.cbar.producer.messaging;

public class MessageSendException extends RuntimeException {

    public MessageSendException(String topic, String key, Throwable cause) {
        super("Message with key=" + key + " was not acknowledged by topic " + topic, cause);
    }
}
