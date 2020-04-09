package com.maas.solace.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerDemoWithKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // Create producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create a producer record

        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "Hello World"+ i;
            String key = "id_"+ i;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            // Send data async
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time the record is successfully  sent

                    if (e == null) {
                        log.info("Received new metadata. \n"
                                + "Topic: " + recordMetadata.topic() + "\n"
                                + "Partition: " + recordMetadata.partition() + "\n"
                                + "TimeStamp: " + recordMetadata.timestamp() + "\n"
                                + "Offset: " + recordMetadata.offset() + "\n");
                    } else {
                        log.error("Error while producing, ", e);
                    }
                }
            }).get();
        }
            // Flush data
            producer.flush();
            // Flush and close data
            producer.close();
        }
}
