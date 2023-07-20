package guru.bonacci.flink.accounts;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SourceAccount {
	
  private String id;
  
  @JsonProperty("poolid")
  private String poolId;
}