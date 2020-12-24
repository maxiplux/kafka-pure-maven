import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ThreadGroupConsumePureKafka extends Thread{

    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ThreadGroupConsumePureKafka(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run()
    {


            consumer.subscribe(Arrays.asList("devs4j-topic"));

            try {
                while(!closed.get())
                {
                    ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String, String> record :records)
                    {
                        log.debug("partition->"+record.partition()+"offset->"+String.valueOf(record.offset())+"key->"+record.key()+"value->"+record.value());

                        if (Integer.parseInt(record.key()) %100 ==0 )
                        {
                            log.info("partition->"+record.partition()+"offset->"+String.valueOf(record.offset())+"key->"+record.key()+"value->"+record.value());
                        }

                    }
                }

            }

            catch(WakeupException e)
            {
                if(!closed.get())
                {
                    throw e;
                }
            }
            finally
            {
                consumer.close();
            }



    }

    public void  shutDown()
    {
        closed.set(true);
        consumer.wakeup();

    }

}
