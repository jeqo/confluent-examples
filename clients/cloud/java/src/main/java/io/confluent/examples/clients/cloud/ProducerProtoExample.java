package io.confluent.examples.clients.cloud;

import io.confluent.cloud.demo.domain.SensorReadingImpl.SensorReading;
import io.confluent.cloud.demo.domain.SensorReadingImpl.SensorReading.Device;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerProtoExample {
    private static Properties configs = new Properties();


    public static void main(String[] args) {
        // Load from the 'ccloud.properties'
        try (InputStream is = new FileInputStream(new File("./ccloud.properties"))) {
            configs.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }

// Append the serialization strategy
        configs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        configs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer.class.getName());

        try (KafkaProducer<String, SensorReading> producer = new KafkaProducer<>(configs)) {
            Device device = Device.newBuilder()
                    .setDeviceID("ABC")
                    .setEnabled(true)
                    .build();
            String recordKey = device.getDeviceID();
            ProducerRecord<String, SensorReading> record =
                    new ProducerRecord<>("SensorReading", recordKey,
                            SensorReading.newBuilder()
                                    .setDevice(device)
                                    .setDateTime(new Date().getTime())
                                    .setReading(new Random().nextDouble())
                                    .build());
            producer.send(record, (metadata, exception) -> {
                System.out.println(String.format(
                        "Reading sent to partition %d with offset %d",
                        metadata.partition(), metadata.offset()));
            });
        }
    }
}
