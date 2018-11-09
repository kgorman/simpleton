package io.eventador;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;

public class FlinkKafkaTable {
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

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        tableEnv
                .connect(
                        new Kafka()
                                .version("0.11")
                                .topic("kg_input")
                                .startFromEarliest()
                                .property("bootstrap.servers", "7f421e3e-kafka0.pub.va.eventador.io:9092")
                )
                .withFormat(
                        new Json()
                                // or by using a JSON schema which parses to DECIMAL and TIMESTAMP
                                .jsonSchema(
                                    "{" +
                                    "  type: 'object'," +
                                    "  properties: {" +
                                    "    sensor: {" +
                                    "      type: 'string'," +
                                    "    }," +
                                    "    temp: {" +
                                    "      type: 'number'" +
                                    "    }" +
                                    "  }" +
                                    "}"
                                )

                )
                .withSchema(
                        new Schema()
                                .field("rowtime", Types.SQL_TIMESTAMP)
                                .proctime()
                                .field("sensor", Types.STRING)
                                .field("temp", Types.LONG)
                )
                .inAppendMode()
                .registerTableSource("mySourceTable");


        Table result2 = tableEnv.sqlQuery("SELECT * FROM mySourceTable WHERE temp > 10");
        String ds = result2.toString();
        System.out.println(ds);

        env.execute("FlinkKafkaTable");
    }
}
