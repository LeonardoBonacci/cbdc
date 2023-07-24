package guru.bonacci.flink;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.OutputTag;

import guru.bonacci.flink.accounts.connect.AccountSources;
import guru.bonacci.flink.accounts.domain.Account;
import guru.bonacci.flink.steps.AccountMatcher;
import guru.bonacci.flink.steps.DynamicBalanceSplitter;
import guru.bonacci.flink.steps.LastRequestCache;
import guru.bonacci.flink.steps.PinotLastRequestProcessed;
import guru.bonacci.flink.steps.PinotSufficientFunds;
import guru.bonacci.flink.steps.RequestThrottler;
import guru.bonacci.flink.steps.gen.RuleGenerator;
import guru.bonacci.flink.transfers.connect.TransferSinks;
import guru.bonacci.flink.transfers.connect.TransferSources;
import guru.bonacci.flink.transfers.domain.Transfer;
import guru.bonacci.flink.transfers.domain.TransferErrors;
import guru.bonacci.flink.transfers.domain.TransferRule;
import guru.bonacci.flink.utils.Splitter;

public class TransferProcessingStreamJob {

	final static String TOPIC_TRANSFER_REQUESTS = "transfer_requests";
	final static String TOPIC_TRANSFERS = "transfers";
	final static String TOPIC_ERRORS = "houston";
	
	
	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		env.setParallelism(10);
		
		Map<String, String> dummyConfig = Map.of("throttle.min.request.difference.sec", "4");
		ParameterTool parameter = ParameterTool.fromMap(dummyConfig);
		
		final OutputTag<Tuple2<Transfer, TransferErrors>> outputTagInvalidTransfer = 
				new OutputTag<Tuple2<Transfer, TransferErrors>>("invalid"){};

		DataStream<TransferRule> ruleStream = env.addSource(new RuleGenerator())
				.map(rule -> {
						System.out.println(rule);
						return rule;
					}
				);

		DataStream<Transfer> transferRequestStream = 
				env.fromSource(TransferSources.kafkaTransferConsumer(TOPIC_TRANSFER_REQUESTS, "flink"), 
												WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source")
					 .filter(tf -> !tf.getFromId().equals(tf.getToId()));

		transferRequestStream.print();
		
		// STEP 1: validate nr. of requests per configured time unit
		DataStream<Tuple2<Transfer, Boolean>> throttlingStream = transferRequestStream
			.keyBy(Transfer::getFromId)	
			.map(new RequestThrottler(parameter.getLong("throttle.min.request.difference.sec"))).name("throttle");
		
		SingleOutputStreamOperator<Transfer> throttledStream = throttlingStream
			  .process(new Splitter(outputTagInvalidTransfer, TransferErrors.TOO_MANY_REQUESTS));

		throttledStream.getSideOutput(outputTagInvalidTransfer) // error handling
		 .map(tuple -> tuple.f1.toString() + ">" + tuple.f0.toString())
		 .sinkTo(TransferSinks.kafkaStringProducer(TOPIC_ERRORS));
		
		KeyedStream<Account, String> accountsChangelogStream = AccountSources.postgresAccountChangelogStream(tableEnv);
		accountsChangelogStream.print();
		
		// STEP 2: validate from-pool combination
		SingleOutputStreamOperator<Tuple2<Transfer, Boolean>> accountsFromStream = throttledStream
			.keyBy(transfer -> transfer.getFromId())
			.connect(accountsChangelogStream)
			.flatMap(new AccountMatcher());

		SingleOutputStreamOperator<Transfer> accountsValidFromStream = accountsFromStream
			  .process(new Splitter(outputTagInvalidTransfer, TransferErrors.FROM_NOT_IN_POOL));

		accountsValidFromStream.getSideOutput(outputTagInvalidTransfer) // error handling
		 .map(tuple -> tuple.f1.toString() + ">" + tuple.f0.toString())
		 .sinkTo(TransferSinks.kafkaStringProducer(TOPIC_ERRORS));

		// STEP 3: validate to-pool combination
		SingleOutputStreamOperator<Tuple2<Transfer, Boolean>> accountsToStream = accountsValidFromStream
				.keyBy(transfer -> transfer.getToId())
				.connect(accountsChangelogStream)
				.flatMap(new AccountMatcher());

		SingleOutputStreamOperator<Transfer> accountsValidToStream = accountsToStream
			  .process(new Splitter(outputTagInvalidTransfer, TransferErrors.TO_NOT_IN_POOL));

		accountsValidToStream.getSideOutput(outputTagInvalidTransfer) // error handling
		 .map(tuple -> tuple.f1.toString() + ">" + tuple.f0.toString())
		 .sinkTo(TransferSinks.kafkaStringProducer(TOPIC_ERRORS));

		// STEP 4: validate amount based on pooltype rules
		@SuppressWarnings("unchecked")
		AsyncRetryStrategy<Tuple2<Transfer, Double>> asyncRetryStrategy =
			new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<Tuple2<Transfer, Double>>(3, 100L) 
				.ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
				.ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
				.build();
		
		 MapStateDescriptor<String, TransferRule> ruleStateDescriptor = 
		      new MapStateDescriptor<>(
		          "RulesBroadcastState",
		          BasicTypeInfo.STRING_TYPE_INFO,
		          TypeInformation.of(new TypeHint<TransferRule>() {}));
		 
		 BroadcastStream<TransferRule> ruleBroadcastStream = ruleStream.broadcast(ruleStateDescriptor);
		 
		 SingleOutputStreamOperator<Transfer> balancedStream = 
				AsyncDataStream.orderedWaitWithRetry(accountsValidToStream.keyBy(transfer -> transfer.getId().toString()), 
																							new PinotSufficientFunds(), 
																							1000, TimeUnit.MILLISECONDS, 100, asyncRetryStrategy)
				.name("sufficient-funds")
				.connect(ruleBroadcastStream)
				.process(new DynamicBalanceSplitter(outputTagInvalidTransfer));

		 balancedStream.getSideOutput(outputTagInvalidTransfer) // error handling
			 .map(tuple -> tuple.f1.toString() + ">" + tuple.f0.toString())
			 .sinkTo(TransferSinks.kafkaStringProducer(TOPIC_ERRORS));

			// STEP 5: validate whether from's previous transfer request has been processed
		 DataStream<Tuple2<Transfer, String>> lastRequestStream = 
				 balancedStream
					.keyBy(Transfer::getFromId)	
					.process(new LastRequestCache()).name("request-cache");

		 SingleOutputStreamOperator<Transfer> dbInSyncStream = 
				 AsyncDataStream.orderedWait(lastRequestStream, 
						 													new PinotLastRequestProcessed(), 
						 													1000, TimeUnit.MILLISECONDS, 1000)
					.name("db-in-sync")
				  .process(new Splitter(outputTagInvalidTransfer, TransferErrors.TRANSFER_IN_PROGRESS));

		 dbInSyncStream.getSideOutput(outputTagInvalidTransfer) // error handling
			 .map(tuple -> tuple.f1.toString() + ">" + tuple.f0.toString())
			 .sinkTo(TransferSinks.kafkaStringProducer(TOPIC_ERRORS));

		 balancedStream.map(new MapFunction<Transfer, Tuple2<Transfer, String>>() {

			@Override
			public Tuple2<Transfer, String> map(Transfer tr) {
				return Tuple2.<Transfer, String>of(tr, tr.getFromId());			}
		 	})
		 	.sinkTo(TransferSinks.kafkaTransferProducer(TOPIC_TRANSFERS));
		 
		env.execute("transfer-request processing");
	}
}
