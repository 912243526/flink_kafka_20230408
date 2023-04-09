package org.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkKafkaConsumerExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        Properties properties = new Properties();

        //指定kafka的Broker地址
        properties.setProperty("bootstrap.servers","192.168.1.226:9092");
        //指定组ID
        properties.setProperty("group.id","gwc10");
        //如果没有记录偏移量，第一次从最开始消费
        properties.setProperty("auto.offset.reset","earliest");
        //kafka的消费者不自动提交偏移量
        //properties.setProperty("enable.auto.commit","false");

        //kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("TEST", new SimpleStringSchema(), properties);

        DataStreamSource<String> lines = env.addSource(kafkaSource);

        //sink
        lines.print();

        env.execute("KafkaSource");


    }
}
