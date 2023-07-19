package guru.bonacci.cbdc.ingress;

import java.util.UUID;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import guru.bonacci.cbdc.ingress.domain.BlockingTransferRequest;
import guru.bonacci.cbdc.ingress.domain.NonBlockingTransferRequest;
import guru.bonacci.cbdc.ingress.pinot.PoolService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.annotation.Nonnull;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.ValidationException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

@Path("/transfers")
public class TransferRequestResource {

	@Channel("transferrequests-out")
	Emitter<BlockingTransferRequest> blockingRequestEmitter;

	@Channel("transferrequests-out")
	Emitter<NonBlockingTransferRequest> nonblockingRequestEmitter;

	@Inject PoolService poolService;
	
	@POST
	@Path("/blocking")
	@Produces(MediaType.TEXT_PLAIN)
	@Consumes(MediaType.APPLICATION_JSON)
	public String blockingRequest(@Valid @Nonnull BlockingTransferRequest req) {
		req.setId(UUID.randomUUID());
		req.setTimestamp(System.currentTimeMillis());
		blockingRequestEmitter.send(req);
		return req.getId().toString();
	}

	// https://github.com/quarkusio/quarkus/discussions/32275

	@POST
	@Path("/nonblocking")
	@Produces(MediaType.TEXT_PLAIN)
	@Consumes(MediaType.APPLICATION_JSON)
	public Uni<Response> nonblockingRequest(@Valid @Nonnull final NonBlockingTransferRequest req) {

		Uni<NonBlockingTransferRequest> validRequest = validate(req).map(valid -> {
			if (!valid) { 
				throw new ValidationException("account not in pool");
			}
			
			req.setId(UUID.randomUUID());
			req.setTimestamp(System.currentTimeMillis());
			return req;
		});
		
		return validRequest.onItem().transformToUni(populatedRequest -> {
			Uni<Void> kafka = Uni
	        .createFrom().completionStage(
	        		nonblockingRequestEmitter.send(populatedRequest));

			Uni<UUID> extractId = Uni.createFrom()
															.item(populatedRequest)
															.map(NonBlockingTransferRequest::getId);

			Uni<Response> processor = extractId.map(uuid -> Response.ok(uuid).build());

			return kafka.replaceWith(processor);
		}).onFailure(ValidationException.class).recoverWithItem(Response.status(Status.BAD_REQUEST).build());
	}
	
	Uni<Boolean> validate(final NonBlockingTransferRequest nonblocking) {
		// How 'much' does it matter?
		Uni<Boolean> fromValid = 
				Uni.createFrom()
				.completionStage(poolService.futureAccountInPool(nonblocking.getFromId(), nonblocking.getPoolId()))
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());

		Uni<Boolean> toValid = Uni.createFrom().item(nonblocking)
        .map(req -> poolService.accountInPool(req.getToId(), req.getPoolId()))
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());

		Uni<Boolean> bothValid = Uni.combine().all().unis(fromValid, toValid)
         .combinedWith(Boolean::logicalAnd);
		 
		 return bothValid;
	}
}