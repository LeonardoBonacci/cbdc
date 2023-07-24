package guru.bonacci.flink.transfers.connect;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;

import guru.bonacci.flink.transfers.domain.Transfer;

public class TransferSources {

	 public static KafkaSource<Transfer> kafkaTransferConsumer(String topic, String kafkaGroup) {
			JsonDeserializationSchema<Transfer> jsonFormat = new JsonDeserializationSchema<>(Transfer.class);
			return KafkaSource.<Transfer>builder()
					    .setBootstrapServers("localhost:9092")
					    .setTopics(topic)
					    .setGroupId(kafkaGroup)
					    .setStartingOffsets(OffsetsInitializer.latest())
			        .setValueOnlyDeserializer(jsonFormat)
			        .build();
	}
	 
	 public static KafkaSource<Tuple2<Transfer, String>> kafkaTransferConsumerTest(String topic, String kafkaGroup) {
			return KafkaSource.<Tuple2<Transfer, String>>builder()
					    .setBootstrapServers("localhost:9092")
					    .setTopics(topic)
					    .setGroupId(kafkaGroup)
					    .setStartingOffsets(OffsetsInitializer.latest())
			        .setDeserializer(new TransferDeserializationSchema())
			        .build();
	}
}
