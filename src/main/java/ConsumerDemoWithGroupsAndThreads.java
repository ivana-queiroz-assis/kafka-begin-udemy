import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithGroupsAndThreads {

    public static void main(String[] args) {

        new ConsumerDemoWithGroupsAndThreads().run();


    }

    private ConsumerDemoWithGroupsAndThreads(){

    }


    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithGroupsAndThreads.class.getName());
        String topic = "first_topic";
        String bootstrapServer = "127.0.0.1:9092";
        String groudId = "my-sixth-application";
        CountDownLatch countDownLatch = new CountDownLatch(1);

        logger.info("Creating the consumer thread.");
        Runnable myConsumerRunnable = new ConsumerRunnable(countDownLatch, bootstrapServer, groudId, topic );

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

         // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Catch shutdown");
            ((ConsumerRunnable) myConsumerRunnable).shutDown();
            try {
                countDownLatch.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Latch wait for the threads.");
        }
        ));

        try {
            countDownLatch.wait();
        } catch (InterruptedException e) {
            logger.info("Application occured a exception.", e);
        } finally {
            logger.info("Application is closing.");
        }
    }

    public class ConsumerRunnable implements Runnable{
        private CountDownLatch countDownLatch ;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(CountDownLatch countDownLatch, String bootstrapServer, String groupId, String topic){
            this.countDownLatch = countDownLatch;
            this.consumer= new KafkaConsumer<String, String>(createProperties(bootstrapServer, groupId));
            // subscribe consumer to a topic
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            // pool for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    records.forEach(r -> {
                        logger.info("Key: " + r.key());
                        logger.info("Partition: " + r.partition());
                        logger.info("Offset: " + r.offset());
                    });
                }
            } catch (WakeupException wakeupException){
                logger.info("Received shutDown signal!");
            } finally {
                consumer.close();
                // tell our main code that we're done with the consumer.
                countDownLatch.countDown();
            }
        }

        public void shutDown(){
            // the method wakeup() is used to stop the polling and will throw the Exception WakeupException
            consumer.wakeup();
        }

        private Properties createProperties(String bootstrapServer, String groupId ){
            //create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return properties;
        }
    }
}
