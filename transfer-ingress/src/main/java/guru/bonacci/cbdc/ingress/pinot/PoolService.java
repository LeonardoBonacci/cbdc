package guru.bonacci.cbdc.ingress.pinot;

import static org.jooq.impl.DSL.field;

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
				.from("accounts_1")
				.where(field("id").eq(accountId).and(field("poolId").eq(poolId)))
				.getSQL(ParamType.INLINED);

		System.out.println(existsQuery);
		ResultSetGroup existsResult = client.getConnection().execute(existsQuery);
		int count =	existsResult.getResultSet(0).getRowCount();
		System.out.println(count);
		return count > 0;
	}
}
