package guru.bonacci.flink.transfers.connect;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonSerializationSchema;

import guru.bonacci.flink.transfers.domain.Transfer;

public class TransferValueSerializationSchema implements SerializationSchema<Tuple2<Transfer, String>> {

	private static final long serialVersionUID = 1L;

	private final JsonSerializationSchema<Transfer> jsonFormat = new JsonSerializationSchema<>();

	@Override
	public void open(InitializationContext context) {
		jsonFormat.open(context);
	}

	@Override
	public byte[] serialize(Tuple2<Transfer, String> element) {
		return jsonFormat.serialize(element.f0);
	}
}