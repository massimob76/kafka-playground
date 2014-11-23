package producer;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestProducer {

    private static final int NO_EVENTS = 10;
    private static final String TOPIC = "my-topic";

    private final ProducerConfig config;

    public TestProducer(Properties props) {
        this.config = new ProducerConfig(props);
    }

    public void produce() {
        Producer<Integer, String> producer = new Producer<>(config);

        for (int key = 0; key < NO_EVENTS; key++) {

            String msg = "no." + key;

            KeyedMessage<Integer, String> data = new KeyedMessage<>(TOPIC, key, msg);
            producer.send(data);
        }
        producer.close();
    }

    public static void main(String[] args) {

        Properties properties = buildProperties();
        TestProducer producer = new TestProducer(properties);
        producer.produce();
    }

    private static Properties buildProperties() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "producer.IntegerEncoder");
        props.put("partitioner.class", "producer.SimplePartitioner");
        props.put("request.required.acks", "1");
        return props;
    }
}
