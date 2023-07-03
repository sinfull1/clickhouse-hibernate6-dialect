package org.hibernate.dialect.function;

import org.hibernate.dialect.Dialect;
import org.hibernate.metamodel.mapping.JdbcMapping;
import org.hibernate.query.sqm.function.AbstractSqmSelfRenderingFunctionDescriptor;
import org.hibernate.query.sqm.function.FunctionKind;
import org.hibernate.query.sqm.produce.function.*;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.SqlAstNodeRenderingMode;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.tree.SqlAstNode;
import org.hibernate.sql.ast.tree.expression.CastTarget;
import org.hibernate.sql.ast.tree.expression.Distinct;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.QueryLiteral;
import org.hibernate.sql.ast.tree.predicate.Predicate;
import org.hibernate.type.*;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.spi.TypeConfiguration;

import java.util.Arrays;
import java.util.List;

import static org.hibernate.query.sqm.produce.function.FunctionParameterType.*;

public class ArraySortFunction extends AbstractSqmSelfRenderingFunctionDescriptor {
    private final SqlAstNodeRenderingMode defaultArgumentRenderingMode;
    private final CastFunction castFunction;
    private final BasicType<Double> doubleType;

    public ArraySortFunction(
            Dialect dialect,
            TypeConfiguration typeConfiguration,
            SqlAstNodeRenderingMode defaultArgumentRenderingMode) {
        super(
                "arraySort",
                FunctionKind.AGGREGATE,
                new ArgumentTypesValidator( StandardArgumentsValidators.max( 3 ), ANY, ANY, ANY ),
                StandardFunctionReturnTypeResolvers.invariant( typeConfiguration.getBasicTypeForJavaType(Object.class)),
                StandardFunctionArgumentTypeResolvers.invariant( typeConfiguration, NUMERIC )
        );
        this.defaultArgumentRenderingMode = defaultArgumentRenderingMode;
        doubleType = typeConfiguration.getBasicTypeRegistry().resolve( StandardBasicTypes.DOUBLE );
        //This is kinda wrong, we're supposed to use findFunctionDescriptor("cast"), not instantiate CastFunction
        //However, since no Dialects currently override the cast() function, it's OK for now
        castFunction = new CastFunction( dialect, dialect.getPreferredSqlTypeCodeForBoolean() );
    }

    @Override
    public void render(SqlAppender sqlAppender, List<? extends SqlAstNode> sqlAstArguments, SqlAstTranslator<?> walker) {
        render( sqlAppender, sqlAstArguments, null, walker );
    }

    @Override
    public void render(
            SqlAppender sqlAppender,
            List<? extends SqlAstNode> sqlAstArguments,
            Predicate filter,
            SqlAstTranslator<?> translator) {
        final boolean caseWrapper = filter != null && !translator.supportsFilterClause();
        sqlAppender.appendSql( "arraySort(" );
        Expression arg;
        Expression arg1 =  null;
        Expression arg2 = null;
        if ( sqlAstArguments.get( 0 ) instanceof Distinct) {
            sqlAppender.appendSql( "distinct " );
            arg = ( (Distinct) sqlAstArguments.get( 0 ) ).getExpression();
        }
        else {
            if ( sqlAstArguments.size() == 3 ) {
                arg = (Expression) sqlAstArguments.get( 0 );
                arg1 = (Expression) sqlAstArguments.get( 1 );
                arg2 = (Expression) sqlAstArguments.get( 2 );
            } else {
                arg = (Expression) sqlAstArguments.get( 0 );
            }
        }
        if ( caseWrapper ) {
            translator.getCurrentClauseStack().push( Clause.WHERE );
            sqlAppender.appendSql( "case when " );
            filter.accept( translator );
            translator.getCurrentClauseStack().pop();
            sqlAppender.appendSql( " then " );
            renderArgument( sqlAppender, translator, arg );
            sqlAppender.appendSql( " else null end)" );
        }
        else {
            if  ( arg1 != null &&  arg2 != null ) {
                sqlAppender.appendSql(((QueryLiteral) arg).getLiteralValue().toString() + ",");
                renderArgument( sqlAppender, translator, arg1 );
                sqlAppender.appendSql(",");
                renderArgument( sqlAppender, translator, arg2 );
            } else {
                renderArgument( sqlAppender, translator, arg );
            }

            sqlAppender.appendSql( ')' );
            if ( filter != null ) {
                translator.getCurrentClauseStack().push( Clause.WHERE );
                sqlAppender.appendSql( " filter (where " );
                filter.accept( translator );
                sqlAppender.appendSql( ')' );
                translator.getCurrentClauseStack().pop();
            }
        }
    }

    private void renderArgument(SqlAppender sqlAppender, SqlAstTranslator<?> translator, Expression realArg) {
        final JdbcMapping sourceMapping = realArg.getExpressionType().getSingleJdbcMapping();
        // Only cast to float/double if this is an integer
        if ( sourceMapping.getJdbcType().isInteger() ) {
            castFunction.render( sqlAppender, Arrays.asList( realArg, new CastTarget(doubleType) ), translator );
        }
        else {
            translator.render( realArg, defaultArgumentRenderingMode );
        }
    }

    @Override
    public String getArgumentListSignature() {
        return "(NUMERIC arg)";
    }
}
