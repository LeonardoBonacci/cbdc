package guru.bonacci.flink.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import guru.bonacci.flink.domain.Transfer;

public class Sinks {

	public static KafkaSink<String> kafkaStringProducer(String topic) {
		return KafkaSink.<String>builder()
         .setBootstrapServers("localhost:9092")
         .setRecordSerializer(KafkaRecordSerializationSchema.builder()
             .setTopic(topic)
             .setValueSerializationSchema(new SimpleStringSchema())
             .build()
         )
         .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
         .build();
	}
	
	public static KafkaSink<Tuple2<Transfer, String>> kafkaTransferProducer(String topic) {
		return KafkaSink.<Tuple2<Transfer, String>>builder()
		    		.setBootstrapServers("localhost:9092")
		        .setRecordSerializer(
		            new KafkaRecordSerializationSchemaBuilder<>()
		            	.setTopic(topic)
	                .setKeySerializationSchema(new TransferKeySerializationSchema())
	                .setValueSerializationSchema(new TransferValueSerializationSchema())
	                .build())
		        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
		        .build();
	}
	
//dbInSyncStream.addSink(
//JdbcSink.sink(
//    "insert into transfers (id, _from, _to, pool_id, amount, timestamp) values (?, ?, ?, ?, ?, ?)",
//    (statement, tf) -> {
//        statement.setString(1, tf.getId().toString());
//        statement.setString(2, tf.getFrom());
//        statement.setString(3, tf.getTo());
//        statement.setString(4, tf.getPoolId());
//        statement.setDouble(5, tf.getAmount());
//        statement.setLong(6, System.currentTimeMillis());
//    },
//    JdbcExecutionOptions.builder()
//            .withBatchSize(1000)
//            .withBatchIntervalMs(200)
//            .withMaxRetries(5)
//            .build(),
//    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//            .withUrl("jdbc:postgresql://localhost:5432/postgres")
//            .withDriverName("org.postgresql.Driver")
//            .withUsername("baeldung")
//            .withPassword("baeldung")
//            .build()
//));

}
