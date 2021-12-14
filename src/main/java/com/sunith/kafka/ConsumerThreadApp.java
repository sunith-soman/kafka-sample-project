package com.sunith.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerThreadApp {
    public static final Logger APP_LOGGER = LoggerFactory.getLogger(ConsumerApp.class);
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String MY_TOPIC = "my_topic";
    public static final String GROUP_ID = "my_consumer_app";

    public static void main(String[] args) {
        ConsumerThreadApp consumer1 = new ConsumerThreadApp();
        consumer1.run();
    }

    public void run(){
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        ConsumerRunnable consRunnable1 = new ConsumerRunnable(BOOTSTRAP_SERVERS,MY_TOPIC,
                GROUP_ID,latch1);
        ConsumerRunnable consRunnable2 = new ConsumerRunnable(BOOTSTRAP_SERVERS,MY_TOPIC,
                GROUP_ID,latch2);
//        Thread consumerThread = new Thread(consRunnable);
//        consumerThread.start();

        ExecutorService ex = Executors.newFixedThreadPool(2);
        ex.submit(consRunnable1);
        ex.submit(consRunnable2);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            APP_LOGGER.info("Shutdown hook");
            consRunnable1.shutdown();
            consRunnable2.shutdown();
            try {
                latch1.await();
                latch2.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            APP_LOGGER.info("Application exited");
        }));

        try {
            latch1.await();
            latch2.await();
        } catch (InterruptedException e) {
            APP_LOGGER.error("Application interrupted",e);
        }
        finally {
            APP_LOGGER.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private final Logger CONSUMER_LOGGER = LoggerFactory.getLogger(ConsumerRunnable.class);
        private String bootstrapServers;
        private String topicName;
        private String groupId;
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;

        public ConsumerRunnable(String bootstrapServers,
                                String topicName,String groupId,
                                CountDownLatch latch){
            this.latch = latch;
            this.bootstrapServers = bootstrapServers;
            this.topicName = topicName;
            this.groupId = groupId;
        }
        @Override
        public void run() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            consumer = new KafkaConsumer<String, String>(props);
            consumer.subscribe(Arrays.asList(topicName));
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        CONSUMER_LOGGER.info(record.key() + " " + record.value() + " " + record.partition() + " " + record.offset());
                    }

                }
            }
            catch(WakeupException e){
                CONSUMER_LOGGER.info("Received shutdown signal");
            }
            finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown(){
            consumer.wakeup();
        }
    }
}
