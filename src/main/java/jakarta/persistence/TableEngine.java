package jakarta.persistence;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface TableEngine {
    String name() default "";

    // for ReplacingMergeTree(cols) kind of cases
    String[] columns() default "";

    // for Redis('redis:6237') kind of cases
    String [] params() default "";

    String ttlColumn() default "";

    String ttlDuration() default "1 DAY";

    String ttlClause() default "";

}
