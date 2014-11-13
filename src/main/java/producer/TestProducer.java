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
        Producer<String, String> producer = new Producer<String, String>(config);

        for (int i = 0; i < NO_EVENTS; i++) {

            String key = String.valueOf(i);
            String msg = "no." + i;

            KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, key, msg);
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
        props.put("metadata.broker.list", "localhost:9093");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "SimplePartitioner");
        props.put("request.required.acks", "1");
        return props;
    }
}
