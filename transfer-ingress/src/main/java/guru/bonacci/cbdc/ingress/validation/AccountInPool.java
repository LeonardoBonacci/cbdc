package guru.bonacci.cbdc.ingress.validation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Constraint(validatedBy = AccountInPoolValidator.class)
public @interface AccountInPool {

    String message() default "Account not in pool";

    Class<? extends Payload>[] payload() default {};

    Class<?>[] groups() default {};
    
}