package guru.bonacci.flink.accounts;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonSerializationSchema;

public class AccountValueSerializationSchema implements SerializationSchema<Tuple2<SinkAccount, String>> {

	private static final long serialVersionUID = 1L;

	private final JsonSerializationSchema<SinkAccount> jsonFormat = new JsonSerializationSchema<>();

	@Override
	public void open(InitializationContext context) {
		jsonFormat.open(context);
	}

	@Override
	public byte[] serialize(Tuple2<SinkAccount, String> element) {
		return jsonFormat.serialize(element.f0);
	}
}