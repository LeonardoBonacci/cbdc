package guru.bonacci.cbdc.ingress;

import java.util.UUID;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import jakarta.annotation.Nonnull;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/transfers")
public class TransferRequestResource {

	@Channel("transferrequests-out")
	Emitter<TransferRequest> requestEmitter;

	
	@POST
	@Path("/request")
	@Produces(MediaType.TEXT_PLAIN)
	@Consumes(MediaType.APPLICATION_JSON)
	public String createRequest(@Valid @Nonnull TransferRequest req) {
		req.setId(UUID.randomUUID());
		req.setTimestamp(System.currentTimeMillis());
		requestEmitter.send(req);
		return req.getId().toString();
	}
}