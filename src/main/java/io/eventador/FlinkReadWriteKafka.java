package io.eventador;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class FlinkReadWriteKafka {
    public static void main(String[] args) throws Exception {
        // Read parameters from command line
        final ParameterTool params = ParameterTool.fromArgs(args);

        if(params.getNumberOfParameters() < 4) {
            System.out.println("\nUsage: FlinkReadWriteKafka --read-topic <topic> --write-topic <topic> --bootstrap.servers <kafka brokers> --group.id <groupid>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(300000); // 300 seconds
        env.getConfig().setGlobalJobParameters(params);

        DataStream<ObjectNode> messageStream = env
                .addSource(new FlinkKafkaConsumer011<>(
                        params.getRequired("read-topic"),
                        new JSONKeyValueDeserializationSchema(true),
                        params.getProperties()));

        messageStream.print();

        messageStream.addSink(new FlinkKafkaProducer011<>(
                params.getRequired("write-topic"),
                new SerializationSchema<ObjectNode>() {
                    @Override
                    public byte[] serialize( ObjectNode element ) {
                        return element.toString().getBytes();
                    }
                },
                params.getProperties())).name("Write To Kafka");

        env.execute("FlinkReadWriteKafka");
    }
}
