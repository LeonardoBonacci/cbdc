package guru.bonacci.flink.accounts;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SinkAccount {
	
  private String id;
  private String poolId;
  private long timestamp;
  
  
  public static SinkAccount from(SourceAccount source) {
  	return SinkAccount.builder()
  			.id(source.getId())
  			.poolId(source.getPoolId())
  			.timestamp(System.currentTimeMillis())
  			.build();
  }
}