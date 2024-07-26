package org.apache.maven;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.graph.streaming.example.ConnectedComponentsExample;
import org.apache.flink.graph.streaming.example.SpannerExample;
import org.apache.flink.graph.streaming.library.Spanner;
import org.apache.flink.graph.streaming.summaries.AdjacencyListGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.SummaryBulkAggregation;
import org.apache.flink.graph.streaming.library.ConnectedComponents;
import org.apache.flink.graph.streaming.summaries.DisjointSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class Kafka_to_Flink_Receiver implements ProgramDescription
{
    public static void main(String[] args) throws Exception
    {
        String kafka_input_topic = "DemoData";
        String kafka_server = "localhost:9092";
        long mergeWindowTime = 1000L;
        long printWindowTime = 2000L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        GraphStream<Long, NullValue, NullValue> edges = getGraphStream(kafka_input_topic, kafka_server, env);
//
//        DataStream<DisjointSet<Long>> cc = edges.aggregate(new ConnectedComponents<>(mergeWindowTime));
//
//        // Flatten the elements of the disjoint set and print
//        // in windows of printWindowTime
//        cc.flatMap(new ConnectedComponentsExample.FlattenSet()).keyBy(0)
//                .timeWindow(Time.of(printWindowTime, TimeUnit.MILLISECONDS))
//                .aggregate(new ConnectedComponentsExample.IdentityAggregate()).print();


//        DataStream<Edge<Long, NullValue>> edges = getGraphStream(kafka_input_topic, kafka_server, env).getEdges();
//        edges.print();
//        edges.map(new MapFunction<Edge<Long, NullValue>, Tuple2<Long, Long>>() {
//            @Override
//            public Tuple2<Long, Long> map(Edge<Long, NullValue> edge) throws Exception {
//                return new Tuple2<>(edge.getSource(), edge.getTarget());
//            }
//        }).keyBy(0).timeWindow(Time.seconds(2), Time.seconds(1)) // Sliding window of size 2 and slides over 1 element
//            .sum(1).print();

        GraphStream<Long, NullValue, NullValue> edges = getGraphStream(kafka_input_topic, kafka_server, env);
        DataStream<AdjacencyListGraph<Long>> spanner = edges.aggregate(new Spanner<Long, NullValue>(mergeWindowTime, 3));

        // flatten the elements of the spanner and
        // in windows of printWindowTime
        spanner.flatMap(new SpannerExample.FlattenSet())
                .keyBy(0).timeWindow(Time.of(printWindowTime, TimeUnit.MILLISECONDS))
                .aggregate(new SpannerExample.IdentityAggregate()).print();
//        spanner.print();

        env.execute("Streaming Connected Components");
    }

    public static GraphStream<Long, NullValue, NullValue> getGraphStream(String inputTopic, String server, StreamExecutionEnvironment env) throws Exception
    {

        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
        DataStream<String> stringInputStream = env.addSource(flinkKafkaConsumer);


        return new SimpleEdgeStream<Long, NullValue>(stringInputStream.map(new MapFunction<String, Edge<Long, NullValue>>()
        {
            private static final long serialVersionUID = -999736771747691234L;

            @Override
            public Edge<Long, NullValue> map(String s)
            {
                String[] fields = s.split("\\s");
                long src = Long.parseLong(fields[0]);
                long trg = Long.parseLong(fields[1]);
                return new Edge<>(src, trg, NullValue.getInstance());
            }
        }), env);

    }

    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(String topic, String kafkaAddress)
    {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        //props.setProperty("group.id",kafkaGroup);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);

        return consumer;
    }

    @Override
    public String getDescription() {
        return "Streaming Connected Components on Global Aggregation";
    }
}