package guru.bonacci.flink.transfers.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransferRule {

	private String poolType;
  private double minBalance;
}