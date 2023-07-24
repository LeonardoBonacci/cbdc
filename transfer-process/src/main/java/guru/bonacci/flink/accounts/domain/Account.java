package guru.bonacci.flink.accounts.domain;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import lombok.Value;

@Value
public class Account {

	private String id;
	private String poolid;
	private RowKind rowKind;
	
	public static Account from(Row row) {
		return new Account(row.getField(0).toString(), 
											 row.getField(1).toString(),
											 row.getKind());
	}
}
