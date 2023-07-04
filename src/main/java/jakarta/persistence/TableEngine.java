package jakarta.persistence;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface TableEngine {
    String name() default "";

    String[] columns() default "";

    String ttlColumn() default "now()";

    String ttlDuration() default "1 DAY";

    String ttlClause() default "";

}
