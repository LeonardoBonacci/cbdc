package guru.bonacci.flink.steps;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import guru.bonacci.flink.accounts.domain.Account;
import guru.bonacci.flink.transfers.domain.Transfer;

public class AccountMatcher extends RichCoFlatMapFunction<Transfer, Account, Tuple2<Transfer, Boolean>> {

	private static final long serialVersionUID = -5024133115979046984L;

	private ValueState<Account> accountState;

	@Override
	public void open(Configuration config) {
	
		accountState =
	          getRuntimeContext()
	                  .getState(new ValueStateDescriptor<>("accounts replica", Account.class));
	}

	@Override
	public void flatMap1(Transfer transfer, Collector<Tuple2<Transfer, Boolean>> out) throws Exception {
	
	  Account account = accountState.value();
	  if (account != null) {
      out.collect(Tuple2.of(transfer, true));
	  } else {
      out.collect(Tuple2.of(transfer, false));
	  }
	}

	@Override
	public void flatMap2(Account account, Collector<Tuple2<Transfer, Boolean>> out) throws Exception {
		if (account.getRowKind() == RowKind.DELETE) {
			accountState.clear();
		} else {
			accountState.update(account);
		}
	}
}