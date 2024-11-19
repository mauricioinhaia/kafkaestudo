package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Callback callback = (data, exception) -> {
            if (Objects.nonNull(exception)) {
                exception.printStackTrace();
                return;
            }

            System.out.println("Sucesso enviando: " + data.topic()
                    + ":::partition " + data.partition()
                    + "/ offset " + data.offset()
                    + "/ timestamp " + data.timestamp());
        };
        var producer = new KafkaProducer<String, String>(properties());
        var value = "0001,65423,700000";
        var record = new ProducerRecord<>("ECOMMERCE_NEWORDER", value, value);
        producer.send(record, callback).get();

        var email = "Thank You! We are processing your order!";
        var emailRecord = new ProducerRecord<>("ECOMMERCE_SENDEMAIL", email, email);
        producer.send(emailRecord, callback).get();
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
