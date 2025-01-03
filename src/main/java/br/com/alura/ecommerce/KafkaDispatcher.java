package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, T> producer;

    KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());

        return properties;
    }

    public void send(String topic,
                     String userId,
                     T value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, userId, value);
        producer.send(record, callback).get();
    }

    private Callback callback = (data, exception) -> {
        if (Objects.nonNull(exception)) {
            exception.printStackTrace();
            return;
        }

        System.out.println("Sucesso enviando: " + data.topic()
                + ":::partition " + data.partition()
                + "/ offset " + data.offset()
                + "/ timestamp " + data.timestamp());
    };

    @Override
    public void close() {
        producer.close();
    }
}
