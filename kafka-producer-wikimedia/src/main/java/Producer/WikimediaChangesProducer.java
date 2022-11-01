package Producer;

import Handler.WikimediaRecentChangeHandler;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class WikimediaChangesProducer {
    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("hello");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer .class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        String topic = "wikimedia.recentchanges";
        EventHandler eventHandler = new WikimediaRecentChangeHandler(producer,topic);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create("https://stream.wikimedia.org/v2/stream/recentchange"));
        EventSource source = builder.build();
        source.start();
        TimeUnit.MINUTES.sleep(10);
    }

}
