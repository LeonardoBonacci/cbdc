package guru.bonacci.flink.accounts;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

public class PostgresDebeziumCDC2Pinot {
	
  public static void main(String[] args) throws Exception {

  	ObjectMapper mapper = new ObjectMapper();
  	
  	Properties debeziumConfig = new Properties();
  	debeziumConfig.put("schema.include.list", false);
  	
  	SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
  	      .hostname("localhost")
  	      .port(5432)
  	      .database("postgres")
          .tableList("public.accounts") 
          .username("baeldung")
          .password("baeldung")
          .decodingPluginName("pgoutput")
  	      .deserializer(new JsonDebeziumDeserializationSchema())
  	      .debeziumProperties(debeziumConfig)
  	      .build();

  	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

  	    DataStream<SinkAccount> accountStream = env
  	      .addSource(sourceFunction)
  	      .map(asStr -> mapper.readTree(asStr))
  	      .map(debeziumJson -> debeziumJson.get("after").toString())
  	      .map(accountJson -> mapper.readValue(accountJson, SourceAccount.class))
  	      .map(SinkAccount::from)
  	      .setParallelism(1);
  	    
  	    
  	    accountStream
  			 	.map(new MapFunction<SinkAccount, Tuple2<SinkAccount, String>>() {

  					@Override
  					public Tuple2<SinkAccount, String> map(SinkAccount account) {
  						return Tuple2.<SinkAccount, String>of(account, account.getId());			}
  				 	})
  	    	.sinkTo(KafkaSink.<Tuple2<SinkAccount, String>>builder()
		    		.setBootstrapServers("localhost:9092")
		        .setRecordSerializer(
		            new KafkaRecordSerializationSchemaBuilder<>()
		            	.setTopic("accounts")
	                .setKeySerializationSchema(new AccountKeySerializationSchema())
	                .setValueSerializationSchema(new AccountValueSerializationSchema())
	                .build())
		        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
		        .build());

  	    accountStream.print();

  	    env.execute();
  } 
}