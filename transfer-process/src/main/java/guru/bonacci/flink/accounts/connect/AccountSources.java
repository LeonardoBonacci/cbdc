package guru.bonacci.flink.accounts.connect;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.RowKind;

import guru.bonacci.flink.accounts.domain.Account;

public class AccountSources {

	 public static KeyedStream<Account, String> postgresAccountChangelogStream(StreamTableEnvironment tableEnv) {

  		tableEnv.executeSql("CREATE TABLE Accounts (\n"
  				+ "   id STRING,\n"
  				+ "   poolid STRING,\n"
  				+ "   PRIMARY KEY (id) NOT ENFORCED\n"
  				+ " ) WITH (\n"
  				+ "   'connector' = 'postgres-cdc',\n"
  				+ "   'hostname' = 'localhost',\n"
  				+ "   'port' = '5432',\n"
  				+ "   'username' = 'baeldung',\n"
  				+ "   'password' = 'baeldung',\n"
  				+ "   'database-name' = 'postgres',\n"
  				+ "   'schema-name' = 'public',\n"
  				+ "   'table-name' = 'accountss',\n"
  				+ "   'slot.name' = 'flink123',\n"
  				+ "   'decoding.plugin.name' = 'pgoutput'\n"
  				+ " );").collect().forEachRemaining(System.out::println);;
  				
			Table accountsTable = tableEnv.sqlQuery("SELECT * FROM Accounts");
			KeyedStream<Account, String> accountStream = 
					tableEnv.toChangelogStream(accountsTable)
									.filter(row -> row.getKind() != RowKind.UPDATE_BEFORE)
									.map(Account::from)
									.keyBy(account -> account.getId());
			
			return accountStream;
	}
}
