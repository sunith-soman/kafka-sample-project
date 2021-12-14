package com.sunith.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerApp {
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static final Logger LOGGER = LoggerFactory.getLogger(ProducerApp.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,"all");
//        props.put(ProducerConfig.RETRIES_CONFIG,Integer.MAX_VALUE);
//        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,5);
//        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
//        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
//        props.put(ProducerConfig.LINGER_MS_CONFIG,5);
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG,32*1024);

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        for(int i=0;i<10;i++) {
            ProducerRecord<String,String> record = new ProducerRecord<>("car_pos","id_"+i,"hello_"+i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        LOGGER.info("Success:" + recordMetadata.topic() + " " + recordMetadata.partition() + " " + recordMetadata.offset() + " " + recordMetadata.timestamp());
                    } else {
                        LOGGER.error("Error:", e);
                    }
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
