package guru.bonacci.cbdc.ingress;

import java.util.UUID;

import guru.bonacci.cbdc.ingress.validation.AccountInPool;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@AccountInPool
public class TransferRequest {
	
  private UUID id;
  
  @NotBlank(message="from may not be blank")
  private String fromId;

  @NotBlank(message="to may not be blank")
  private String toId;

  @NotBlank(message="pool may not be blank")
  private String poolId;

  @NotBlank(message="poolType may not be blank")
  private String poolType;
  
  @Min(message="Cheating...", value=0)
  private double amount;
  
  private long timestamp;
}