package com.example.rt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.base.DeliveryGuarantee;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashSet;

/** Realtime PV/UV per minute, write to Kafka topic `events_clean`. */
public class PVUVPerMinute{

    // 简单事件对象，只保留统计需要的字段
    public static class Event{
        public long eventTimeMillis;
        public String userId;

        public static Event fromJson(String json, ObjectMapper om){ // 把 Kafka的 JSON字符串数据 转换成 Java对象 (Event)，让 FLink能识别和处理
            try{
                JsonNode n = om.readTree(json); // JsonNode是 Jackson提供的一个通用树节点类，代表“任意JSON结构”
                String ts = n.path("event_time").asText(null);
                if(ts == null){
                    return null;
                }

                // Flink 要求用 long 型毫秒时间戳 来标识事件时间
                long millis;
                try{
                    // Instant 用来表示 UTC时间点
                    // 兼容 “+00:00” -> "Z"， 便于 Instant.parse
                    // toEpochMilli 将时间转换成 毫秒时间戳
                    millis = Instant.parse(ts.replace("+00:00", "Z")).toEpochMilli();
                }
                catch(DateTimeParseException ex){
                    return null;
                }

                Event e = new Event();
                e.eventTimeMillis = millis;
                e.userId = n.path("user_id").asText(null);
                if(e.userId == null){
                    return null;
                }
                
                return e;
            }
            catch(Exception ex){
                return null;
            }
        }
    }

    public static void main(String[] args) throws Exception{
        final String bootstrap = "kafka:9092";
        final String inTopic = "events_raw";
        final String outTopic = "events_clean";
        final String groupId = "flink-pvuv-01";

        // create Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. Kafka source configuration
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("Kafka:9092")
            .setTopics("events_raw")
            .setGroupId("flink-pvuv-01")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // add Kafka source to the Flink env
        DataStreamSource<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka-events_raw");

        // 2. parse JSON into Java objects
        ObjectMapper om = new ObjectMapper();

        // convert JSON string -> Event objects -> filter invalid ones -> assign event time and watermarks
        var events = raw
            .map(s -> Event.fromJson(s, om))
            .filter(e -> e != null)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((e, ts) -> e.eventTimeMillis)
            );

        // 3. process all events in 1-minute tumbling windows
        var result = events
            .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
            .process(new ProcessAllWindowFunction<Event, String, TimeWindow>(){
                @Override
                public void process(Context context, Iterable<Event> elements, Collector<String> out){

                    long pv = 0; // page views
                    HashSet<String> uvSet = new HashSet<>(); // track unique user IDs

                    for(Event e : elements){
                        pv++;
                        uvSet.add(e.userId);
                    }

                    long uv = uvSet.size();

                    // format result with readable window time range
                    String resultJson = String.format(
                        "{\"window_start\":\"%s\",\"window_end\":\"%s\",\"pv\":%d,\"uv\":%d}",
                        Instant.ofEpochMilli(context.window().getStart()),
                        Instant.ofEpochMilli(context.window().getEnd()),
                        pv, uv
                    );

                    out.collect(resultJson);
                }
            });

        // 4. write window results to Kafka topic "events_clean"
        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(outTopic)
                    .setValueSerializationSchema(
                        (String v) -> v.getBytes(StandardCharsets.UTF_8))
                    .build()   
            )
            .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        // sink
        result.sinkTo(sink);

        // print to console
        result.print();

        // execute the Flink job
        env.execute("PV and UV per Minute");
    }
}

