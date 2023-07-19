package guru.bonacci.cbdc.ingress;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TransferRequestsGen {

	private final static List<String> USERS = 
			List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");

	private final static List<String> POOL_TYPES = 
			List.of("easy"); //, RuleGenerator.MEDIUM, RuleGenerator.HARD);

	private final Random random = new Random();

	
	@Outgoing("transferrequests-out")
	public Multi<TransferRequest> generate() {
		return Multi.createFrom().ticks().every(Duration.ofSeconds(1))
				.map(ignore -> new TransferRequest(
													UUID.randomUUID(), // id
													USERS.get(random.nextInt(USERS.size())), // from
													USERS.get(random.nextInt(USERS.size())), // to
													"coro", // poolId
													POOL_TYPES.get(random.nextInt(POOL_TYPES.size())), // poolType
													random.nextDouble(0, 100), // amount
													System.currentTimeMillis() // timestamp
												));
	}	
}