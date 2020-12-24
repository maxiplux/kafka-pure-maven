import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class PureProducer {
    public static void main(String[] args) {
        Properties props=new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("acks","1");

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        try(Producer<String, String> producer=new
                KafkaProducer<>(props);) {
            for(int currentIndexKey= 0; currentIndexKey< 100*1_000;currentIndexKey++) {
                //producer.send(new ProducerRecord<>("devs4j-topic","key","message"+i)).get(); // sincrona
                producer.send(new ProducerRecord<>("devs4j-topic",""+currentIndexKey,"message"+currentIndexKey)); // asincrona
            }
            producer.flush();



        }
    }
}
