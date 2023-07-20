package guru.bonacci.flink.accounts;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;

public class AccountKeySerializationSchema implements SerializationSchema<Tuple2<SinkAccount, String>> {

	private static final long serialVersionUID = 1L;

	private final SimpleStringSchema stringFormat = new SimpleStringSchema();

	@Override
	public void open(InitializationContext context) throws Exception {
		stringFormat.open(context);
	}

	@Override
	public byte[] serialize(Tuple2<SinkAccount, String> element) {
		try { 
			return stringFormat.serialize(element.f1);
		} catch(Throwable t) {
			t.printStackTrace();
			throw t;
		}
	}
}
