package com.sunith.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerApp {
    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumerApp.class);
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String MY_TOPIC = "car_pos";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "new_consumer_app");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,3);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,3000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,10000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,3000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(MY_TOPIC));
        int pollCount=0;
        while(true){
            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            pollCount++;
            LOGGER.info(pollCount+" : "+consumerRecords.count());
            for(ConsumerRecord<String,String> record:consumerRecords){
                LOGGER.info(record.key()+" "+record.value()+" "+record.partition()+" "+record.offset());
            }
            consumer.commitSync();
        }
    }
}
