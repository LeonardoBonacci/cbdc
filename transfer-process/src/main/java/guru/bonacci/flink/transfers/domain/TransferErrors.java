package guru.bonacci.flink.transfers.domain;

public enum TransferErrors {

	TOO_MANY_REQUESTS,
	FROM_NOT_IN_POOL,
	TO_NOT_IN_POOL,
	INSUFFICIENT_BALANCE,
	TRANSFER_IN_PROGRESS;
}
