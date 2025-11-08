package com.example.rt;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadKafkaPrint{

    public static void main(String[] args) throws Exception{
        // Flink runs inside Docker; use container hostname = internal port.
        final String bootstrapServers = "Kafka:9092";
        final String topic = "events_raw";
        final String groupId = "flink-reader-01";

        // 1) Execution env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 2) Kafka source (read earliest to see existing data)
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 反序列化器
                .build();

        // 3) Build stream and print raw JSON
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-events_raw");

        stream.name("print-raw-json").print();

        // 4) Execute
        env.execute("Read Kafka and Print"); // execute()可选参数：JobName
    }
}
