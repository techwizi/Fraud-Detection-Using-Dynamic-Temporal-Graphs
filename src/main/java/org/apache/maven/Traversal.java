package org.apache.maven;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.streaming.example.SpannerExample;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.summaries.AdjacencyListGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Traversal implements ProgramDescription
{
    public static void main(String[] args) throws Exception
    {
        String kafka_input_topic = "DemoData";
        String kafka_server = "localhost:9092";
        long windowTime = 5000L;
        long slideTime = 2000L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        GraphStream<Long, NullValue, NullValue> graphStream = getGraphStream(kafka_input_topic, kafka_server, env);

        DataStream<Edge<Long, NullValue>> edges = graphStream.getEdges();

        DataStream<Edge<Long, NullValue>> timedEdges = edges
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Edge<Long, NullValue>>() {
                    @Override
                    public long extractAscendingTimestamp(Edge<Long, NullValue> edge) {
                        // Extract timestamp from the edge, assuming edges have timestamps
                        return System.currentTimeMillis();
                    }
                });
//                    .assignTimestampsAndWatermarks(
//                            WatermarkStrategy
//                                .<Edge<Long, NullValue>>forBoundedOutOfOrderness(Duration.ofMillis(slideTime))
//                                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
//                    );


        edges.process(new ProcessFunction<Edge<Long, NullValue>, Edge<Long, NullValue>>() {
            @Override
            public void processElement(Edge<Long, NullValue> edge, Context ctx, Collector<Edge<Long, NullValue>> out) {
                long timestamp = ctx.timestamp();
                System.out.println("Element: " + edge + ", Timestamp: " + timestamp);
                out.collect(edge);
            }
        });

        DataStream<AdjacencyListGraph<Long>> adjGraphStream = timedEdges
//                .process(new ProcessFunction<Edge<Long, NullValue>, AdjacencyListGraph<Long>>()
//                {
//                    private transient ListState<Edge<Long, NullValue>> edgesState;
//                    private transient ValueState<Long> timerState;
//                    @Override
//                    public void open(Configuration parameters)
//                    {
//                        ListStateDescriptor<Edge<Long, NullValue>> edgesStateDescriptor = new ListStateDescriptor<>("edgesState", TypeInformation.of(new TypeHint<Edge<Long, NullValue>>() {}));
//                        edgesState = getRuntimeContext().getListState(edgesStateDescriptor);
//
//                        ValueStateDescriptor<Long> timerStateDescriptor = new ValueStateDescriptor<>("timerState", Long.class);
//                        timerState = getRuntimeContext().getState(timerStateDescriptor);
//                    }
//                    @Override
//                    public void processElement(Edge<Long, NullValue> value, ProcessFunction<Edge<Long, NullValue>, AdjacencyListGraph<Long>>.Context ctx, Collector<AdjacencyListGraph<Long>> collector) throws Exception
//                    {
//                        // Add the edge to the state
//                        edgesState.add(value);
//
//                        // Register a processing time timer if not already set
//                        if (timerState.value() == null) {
//                            long timerTimestamp = ctx.timerService().currentProcessingTime() + 5000; // 5 seconds
//                            ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
//                            timerState.update(timerTimestamp);
//                        }
//                    }
//                    @Override
//                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdjacencyListGraph<Long>> out) throws Exception {
//                        // Create adjacency list
//                        AdjacencyListGraph<Long> graph = new AdjacencyListGraph<>();
//
//                        for (Edge<Long, NullValue> edge : edgesState.get()) {
//                            graph.addEdge(edge.getSource(), edge.getTarget());
//                        }
//
//                        // Emit the adjacency list
//                        out.collect(graph);
//
//                        // Clear the state
//                        edgesState.clear();
//                        timerState.clear();
//                    }
//                });
                .windowAll(SlidingProcessingTimeWindows.of(Time.milliseconds(windowTime), Time.milliseconds(slideTime)))
//                .trigger(PurgingTrigger.of(CountTrigger.of(5)))
//                .process(new ProcessAllWindowFunction<Edge<Long, NullValue>, AdjacencyListGraph<Long>, TimeWindow>() {
//                    @Override
//                    public void process(Context ctx, Iterable<Edge<Long, NullValue>> elements, Collector<AdjacencyListGraph<Long>> out) {
//                        AdjacencyListGraph<Long> graph = new AdjacencyListGraph<>();
//                        for (Edge<Long, NullValue> edge : elements) {
//                            System.out.println("Processing edge: " + edge.getSource() + " -> " + edge.getTarget());
//                            graph.addEdge(edge.getSource(), edge.getTarget());
//                        }
//                        System.out.println("Window processed with " + graph.size() + " edges");
//                        out.collect(graph);
//                    }
//                });
//
//        adjGraphStream.print();

                .aggregate(new AggregateFunction<Edge<Long, NullValue>, AdjacencyListGraph<Long>, AdjacencyListGraph<Long>>() {

                    @Override
                    public AdjacencyListGraph<Long> createAccumulator() {
                        System.out.println("Creating Adjacency List");
                        return new AdjacencyListGraph<>();
                    }

                    @Override
                    public AdjacencyListGraph<Long> add(Edge<Long, NullValue> edge, AdjacencyListGraph<Long> graph) {
                        System.out.println("Adding Edge: " + edge);
                        graph.addEdge(edge.getSource(), edge.getTarget());
                        return graph;
                    }

                    @Override
                    public AdjacencyListGraph<Long> getResult(AdjacencyListGraph<Long> graph) {
                        System.out.println("Getting Result");
                        return graph;
                    }

                    @Override
                    public AdjacencyListGraph<Long> merge(AdjacencyListGraph<Long> graph1, AdjacencyListGraph<Long> graph2) {
                        System.out.println("Merging Adjacency List");
                        return graph1;
                    }
                });

        adjGraphStream.map((MapFunction<AdjacencyListGraph<Long>, String>) graph -> {
                if(graph.patternFound(1L, 2L))
                    return "Found edge 1 -> 2 in this window";
                else
                    return "Did not find edge 1 -> 2 in this window";
        }).print();


        env.execute("Traversal");
    }


    public static GraphStream<Long, NullValue, NullValue> getGraphStream(String inputTopic, String server, StreamExecutionEnvironment env) throws Exception
    {
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
        DataStream<String> stringInputStream = env.addSource(flinkKafkaConsumer);


        return new SimpleEdgeStream<>(stringInputStream.map(new MapFunction<>() {
            private static final long serialVersionUID = -999736771747691234L;

            @Override
            public Edge<Long, NullValue> map(String s) {
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

