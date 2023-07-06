package org.hibernate.dialect;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.sqm.ComparisonOperator;
import org.hibernate.sql.ast.spi.AbstractSqlAstTranslator;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.expression.CastTarget;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.Literal;
import org.hibernate.sql.ast.tree.expression.Summarization;
import org.hibernate.sql.ast.tree.from.QueryPartTableReference;
import org.hibernate.sql.ast.tree.from.ValuesTableReference;
import org.hibernate.sql.ast.tree.predicate.BooleanExpressionPredicate;
import org.hibernate.sql.ast.tree.predicate.LikePredicate;
import org.hibernate.sql.ast.tree.select.QueryGroup;
import org.hibernate.sql.ast.tree.select.QueryPart;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.exec.spi.JdbcOperation;


import java.util.Locale;


//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//
public class ClickHouseSqlAstTranslator<T extends JdbcOperation> extends AbstractSqlAstTranslator<T> {
    public ClickHouseSqlAstTranslator(SessionFactoryImplementor sessionFactory, Statement statement) {
        super(sessionFactory, statement);
    }

    public static String getSqlType(CastTarget castTarget, Dialect dialect) {
        String sqlType = castTarget.getSqlType();
        if (sqlType != null) {
            int parenthesesIndex = sqlType.indexOf(40);
            String baseName = parenthesesIndex == -1 ? sqlType : sqlType.substring(0, parenthesesIndex);
            switch (baseName.toLowerCase(Locale.ROOT)) {
                case "bit":
                    return "unsigned";
                case "tinyint":
                case "smallint":
                case "integer":
                case "bigint":
                    return "signed";
                case "float":
                case "real":
                case "double precision":
                    int precision = castTarget.getPrecision() == null ? dialect.getDefaultDecimalPrecision() : castTarget.getPrecision();
                    int scale = castTarget.getScale() == null ? 2 : castTarget.getScale();
                    return "decimal(" + precision + "," + scale + ")";
                case "char":
                case "varchar":
                case "nchar":
                case "nvarchar":
                    return "char";
                case "binary":
                case "varbinary":
                    return "binary";
            }
        }

        return sqlType;
    }

    protected void renderExpressionAsClauseItem(Expression expression) {
        expression.accept(this);
    }

    protected void visitRecursivePath(Expression recursivePath, int sizeEstimate) {
        if (sizeEstimate == -1) {
            super.visitRecursivePath(recursivePath, sizeEstimate);
        } else {
            this.appendSql("cast(");
            recursivePath.accept(this);
            this.appendSql(" as char(");
            this.appendSql(sizeEstimate);
            this.appendSql("))");
        }

    }

    public void visitBooleanExpressionPredicate(BooleanExpressionPredicate booleanExpressionPredicate) {
        boolean isNegated = booleanExpressionPredicate.isNegated();
        if (isNegated) {
            this.appendSql("not(");
        }

        booleanExpressionPredicate.getExpression().accept(this);
        if (isNegated) {
            this.appendSql(')');
        }

    }

    protected String getForShare(int timeoutMillis) {
        return this.getDialect().getVersion().isSameOrAfter(8) ? " for share" : " lock in share mode";
    }

    protected boolean shouldEmulateFetchClause(QueryPart queryPart) {
        return this.useOffsetFetchClause(queryPart) && this.getQueryPartForRowNumbering() != queryPart && this.getDialect().supportsWindowFunctions() && !this.isRowsOnlyFetchClauseType(queryPart);
    }

    public void visitQueryGroup(QueryGroup queryGroup) {
        if (this.shouldEmulateFetchClause(queryGroup)) {
            this.emulateFetchOffsetWithWindowFunctions(queryGroup, true);
        } else {
            super.visitQueryGroup(queryGroup);
        }

    }

    public void visitQuerySpec(QuerySpec querySpec) {
        if (this.shouldEmulateFetchClause(querySpec)) {
            this.emulateFetchOffsetWithWindowFunctions(querySpec, true);
        } else {
            super.visitQuerySpec(querySpec);
        }

    }

    public void visitValuesTableReference(ValuesTableReference tableReference) {
        this.emulateValuesTableReferenceColumnAliasing(tableReference);
    }

    public void visitQueryPartTableReference(QueryPartTableReference tableReference) {
        if (this.getDialect().getVersion().isSameOrAfter(8)) {
            super.visitQueryPartTableReference(tableReference);
        } else {
            this.emulateQueryPartTableReferenceColumnAliasing(tableReference);
        }

    }

    public void visitOffsetFetchClause(QueryPart queryPart) {
        if (!this.isRowNumberingCurrentQueryPart()) {
            this.renderCombinedLimitClause(queryPart);
        }

    }

    protected void renderComparison(Expression lhs, ComparisonOperator operator, Expression rhs) {
        this.renderComparisonDistinctOperator(lhs, operator, rhs);
    }

    protected void renderPartitionItem(Expression expression) {
        if (expression instanceof Literal) {
            this.appendSql("'0'");
        } else if (expression instanceof Summarization) {
            Summarization summarization = (Summarization) expression;
            this.renderCommaSeparated(summarization.getGroupings());
            this.appendSql(" with ");
            this.appendSql(summarization.getKind().sqlText());
        } else {
            expression.accept(this);
        }

    }

    public void visitLikePredicate(LikePredicate likePredicate) {
        if (this.getDialect().getVersion().isSameOrAfter(8, 0, 24)) {
            super.visitLikePredicate(likePredicate);
            if (!this.getDialect().isNoBackslashEscapesEnabled() && likePredicate.getEscapeCharacter() == null) {
                this.appendSql(" escape ''");
            }
        } else {
            if (likePredicate.isCaseSensitive()) {
                likePredicate.getMatchExpression().accept(this);
                if (likePredicate.isNegated()) {
                    this.appendSql(" not");
                }

                this.appendSql(" like ");
                this.renderBackslashEscapedLikePattern(likePredicate.getPattern(), likePredicate.getEscapeCharacter(), this.getDialect().isNoBackslashEscapesEnabled());
            } else {
                this.appendSql(this.getDialect().getLowercaseFunction());
                this.appendSql('(');
                likePredicate.getMatchExpression().accept(this);
                this.appendSql(')');
                if (likePredicate.isNegated()) {
                    this.appendSql(" not");
                }

                this.appendSql(" like ");
                this.appendSql(this.getDialect().getLowercaseFunction());
                this.appendSql('(');
                this.renderBackslashEscapedLikePattern(likePredicate.getPattern(), likePredicate.getEscapeCharacter(), this.getDialect().isNoBackslashEscapesEnabled());
                this.appendSql(')');
            }

            if (likePredicate.getEscapeCharacter() != null) {
                this.appendSql(" escape ");
                likePredicate.getEscapeCharacter().accept(this);
            }
        }

    }

    public boolean supportsRowValueConstructorSyntaxInSet() {
        return false;
    }

    public boolean supportsRowValueConstructorSyntaxInInList() {
        return true;
    }

    protected boolean supportsRowValueConstructorSyntaxInQuantifiedPredicates() {
        return false;
    }

    protected boolean supportsIntersect() {
        return false;
    }

    protected boolean supportsDistinctFromPredicate() {
        return true;
    }

    protected boolean supportsSimpleQueryGrouping() {
        return this.getDialect().getVersion().isSameOrAfter(8);
    }

    protected boolean supportsNestedSubqueryCorrelation() {
        return false;
    }

    protected boolean supportsWithClause() {
        return this.getDialect().getVersion().isSameOrAfter(8);
    }

    protected String getFromDual() {
        return " from dual";
    }

    protected String getFromDualForSelectOnly() {
        return this.getDialect().getVersion().isSameOrAfter(8) ? "" : this.getFromDual();
    }

    public ClickHouseDialect getDialect() {
        return (ClickHouseDialect) DialectDelegateWrapper.extractRealDialect(super.getDialect());
    }

    public void visitCastTarget(CastTarget castTarget) {
        String sqlType = getSqlType(castTarget, this.getDialect());
        if (sqlType != null) {
            this.appendSql(sqlType);
        } else {
            super.visitCastTarget(castTarget);
        }

    }
}
