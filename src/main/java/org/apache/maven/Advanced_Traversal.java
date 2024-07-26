package org.apache.maven;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.streaming.summaries.AdjacencyGraph;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.NullValue;

import java.util.Properties;

public class Advanced_Traversal implements ProgramDescription
{
    public static void main(String[] args) throws Exception
    {
        String kafka_input_topic = "DemoData";
        String kafka_server = "localhost:9092";
        long windowTime = 30000L;
        long slideTime = 10000L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        GraphStream<Long, NullValue, Tuple2<Long, Long>> graphStream = getGraphStream(kafka_input_topic, kafka_server, env);

        DataStream<Edge<Long, Tuple2<Long, Long>>> edges = graphStream.getEdges();

        DataStream<Edge<Long, Tuple2<Long, Long>>> timedEdges = edges
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Edge<Long, Tuple2<Long, Long>>>() {
                    @Override
                    public long extractAscendingTimestamp(Edge<Long, Tuple2<Long, Long>> edge) {
                        // Extract timestamp from the edge, assuming edges have timestamps
                        return edge.getValue().f1;
                    }
                });



//        edges.process(new ProcessFunction<Edge<Long, NullValue>, Edge<Long, NullValue>>() {
//            @Override
//            public void processElement(Edge<Long, NullValue> edge, Context ctx, Collector<Edge<Long, NullValue>> out) {
//                long timestamp = ctx.timestamp();
//                System.out.println("Element: " + edge + ", Timestamp: " + timestamp);
//                out.collect(edge);
//            }
//        });

        DataStream<AdjacencyGraph<Long, Long, Long>> adjGraphStream = timedEdges
                .windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(windowTime)))
                .aggregate(new AggregateFunction<Edge<Long, Tuple2<Long, Long>>, AdjacencyGraph<Long, Long, Long>, AdjacencyGraph<Long, Long, Long>>() {

                    @Override
                    public AdjacencyGraph<Long, Long, Long> createAccumulator() {
//                        System.out.println("Creating Adjacency List");
                        return new AdjacencyGraph<>();
                    }

                    @Override
                    public AdjacencyGraph<Long, Long, Long> add(Edge<Long, Tuple2<Long, Long>> edge, AdjacencyGraph<Long, Long, Long> graph) {
//                        System.out.println("Adding Edge: " + edge);
                        graph.addEdge(edge.getSource(), edge.getTarget(), edge.getValue().f0, edge.getValue().f1);
                        return graph;
                    }

                    @Override
                    public AdjacencyGraph<Long, Long, Long> getResult(AdjacencyGraph<Long, Long, Long> graph) {
//                        System.out.println("Getting Result");
                        return graph;
                    }

                    @Override
                    public AdjacencyGraph<Long, Long, Long> merge(AdjacencyGraph<Long, Long, Long> graph1, AdjacencyGraph<Long, Long, Long> graph2) {
//                        System.out.println("Merging Adjacency List");
                        return graph1;
                    }
                });

        FraudPatterns fraudPatterns = new FraudPatterns();
        DataStream<String> SinkStream = adjGraphStream.map((MapFunction<AdjacencyGraph<Long, Long, Long>, String>) fraudPatterns::MoneyMovement);


        env.execute("Traversal");
    }


    public static GraphStream<Long, NullValue, Tuple2<Long, Long>> getGraphStream(String inputTopic, String server, StreamExecutionEnvironment env) throws Exception
    {
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
        DataStream<String> stringInputStream = env.addSource(flinkKafkaConsumer);


        return new SimpleEdgeStream<>(stringInputStream.map(new MapFunction<>() {
            private static final long serialVersionUID = -999736771747691234L;

            @Override
            public Edge<Long, Tuple2<Long, Long>> map(String s) {
                String[] fields = s.split("\\s");
                long src = Long.parseLong(fields[0]);
                long trg = Long.parseLong(fields[1]);
                long weight = Long.parseLong(fields[2]);
                long timestamp = Long.parseLong(fields[3]);
                return new Edge<>(src, trg, new Tuple2<>(weight, timestamp));
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
        return "Traversal of weighted dynamic temporal graph to detect fraud patterns";
    }
}

