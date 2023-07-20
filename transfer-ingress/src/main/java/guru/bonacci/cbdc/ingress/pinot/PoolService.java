package guru.bonacci.cbdc.ingress.pinot;

import static org.jooq.impl.DSL.field;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.pinot.client.ResultSetGroup;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class PoolService {

	@Inject PinotClient client;
	
	public boolean accountInPool(String accountId, String poolId) {
		String existsQuery = DSL.using(SQLDialect.POSTGRES)
				.select(field("*"))
				.from("accounts")
				.where(field("id").eq(accountId).and(field("poolId").eq(poolId)))
				.getSQL(ParamType.INLINED);

		ResultSetGroup existsResult = client.getConnection().execute(existsQuery);
		int count =	existsResult.getResultSet(0).getRowCount();
		return count > 0;
	}
	
	public CompletableFuture<Boolean> futureAccountInPool(String accountId, String poolId) {
		String existsQuery = DSL.using(SQLDialect.POSTGRES)
				.select(field("*"))
				.from("accounts")
				.where(field("id").eq(accountId).and(field("poolId").eq(poolId)))
				.getSQL(ParamType.INLINED);

  	Future<ResultSetGroup> result = client.getConnection().executeAsync(existsQuery);
  	return CompletableFuture.supplyAsync(new Supplier<ResultSetGroup>() {

        @Override
        public ResultSetGroup get() {
            try {
                return result.get();
            } catch (InterruptedException | ExecutionException e) {
                return null;
            }
        }
    }).thenApply(existsResult -> {
			return existsResult.getResultSet(0).getRowCount() > 0;
    });
	}
}
