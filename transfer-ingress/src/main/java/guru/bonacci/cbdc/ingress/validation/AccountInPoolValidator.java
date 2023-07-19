package guru.bonacci.cbdc.ingress.validation;

import guru.bonacci.cbdc.ingress.TransferRequest;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AccountInPoolValidator implements ConstraintValidator<AccountInPool, Object> {

	@Inject PoolService service;
	 
  @Override
  public void initialize(AccountInPool constraintAnnotation) {}
  
  @Override
  public boolean isValid(Object value, ConstraintValidatorContext constraintValidatorContext) {
  	// other validations guarantee that value is not null

    try {
    	
    	if (value instanceof TransferRequest transferRequest) {

    		return service.accountInPool(transferRequest.getFromId(), transferRequest.getPoolId())
    				&& service.accountInPool(transferRequest.getToId(), transferRequest.getPoolId());
    	} else {
      	return false;
    	}
    } catch (Exception error) {
    	log.info(error.getMessage());
      return false;
    }
  }
}  