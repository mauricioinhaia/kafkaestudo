package br.com.alura.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var dispatcher = new KafkaDispatcher()){
            for (int i = 0; i < 10; i++) {
                var userId = UUID.randomUUID().toString();
                var value = userId + ",65423,700000";
                dispatcher.send("ECOMMERCE_NEWORDER",userId, value);

                var email = "Thank You! We are processing your order!";
                dispatcher.send("ECOMMERCE_SENDEMAIL", userId, email);
            }
        }
    }
}
