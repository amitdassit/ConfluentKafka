package com.linuxacademy.ccdak.schemaregistry;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SchemaRegistryConsumerMain {


    public static void main(String[] args) {
        System.out.println("In class");
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "20.42.96.64:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://20.42.96.64:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        System.out.println("In properties");
        KafkaConsumer<String, Person> consumer = new KafkaConsumer<>(props);
        System.out.println("In Kafkaconsumer");
        consumer.subscribe(Collections.singletonList("employees"));
        System.out.println("Consumer : "+consumer);
        System.out.println("In subscribe");
//        Map<String, List<PartitionInfo>> topics;
//        topics = consumer.listTopics();
//        System.out.println("**************Topics : "+topics);


        while (true) {



            final ConsumerRecords<String, Person> records = consumer.poll(Duration.ofMillis(100));
            //System.out.println(records);

            for (final ConsumerRecord<String, Person> record : records) {
                System.out.println(record);
                final String key = record.key();
                final Person value = record.value();
                System.out.println("key=" + key + ", value=" + value);
            }

        }
    }

}