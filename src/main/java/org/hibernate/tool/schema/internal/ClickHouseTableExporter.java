package org.hibernate.tool.schema.internal;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//


import jakarta.persistence.PartitionKey;
import jakarta.persistence.TableEngine;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.QualifiedNameParser;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.dialect.Dialect;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.mapping.ClickHousePrimaryKey;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Table;
import org.hibernate.tool.schema.spi.SchemaManagementException;

import java.util.*;

public class ClickHouseTableExporter extends StandardTableExporter {
    protected final Dialect dialect;

    public ClickHouseTableExporter(Dialect dialect) {
        super(dialect);
        this.dialect = dialect;
    }

    private static QualifiedName getTableName(Table table) {
        return new QualifiedNameParser.NameParts(Identifier.toIdentifier(table.getCatalog(), table.isCatalogQuoted()), Identifier.toIdentifier(table.getSchema(), table.isSchemaQuoted()), table.getNameIdentifier());
    }

    private PersistentClass findPersistentClass(String tableName, Metadata metadata) {
        for (PersistentClass persistentClass : metadata.getEntityBindings()) {
            if (tableName.equalsIgnoreCase(persistentClass.getTable().getName())) {
                return persistentClass;
            }
        }
        return null;
    }

    @Override
    public String[] getSqlCreateStrings(Table table, Metadata metadata, SqlStringGenerationContext context) {
        QualifiedName tableName = getTableName(table);
        PersistentClass persistentClass = findPersistentClass(table.getName(), metadata);
        if (persistentClass == null) {
            throw new SchemaManagementException("No entity binding found for table: " + table.getName());
        }
        Class<?> entityClass = persistentClass.getMappedClass();
        TableEngine tableEngine = entityClass.getAnnotation(TableEngine.class);
        PartitionKey partitionKey = entityClass.getAnnotation(PartitionKey.class);

        try {
            String formattedTableName = context.format(tableName);
            StringBuilder createTable = (new StringBuilder(this.tableCreateString(table.hasPrimaryKey()))).append(' ').append(formattedTableName).append(" (");
            boolean isFirst = true;

            Column column;
            for (Iterator var8 = table.getColumns().iterator(); var8.hasNext(); ColumnDefinitions.appendColumn(createTable, column, table, metadata, this.dialect, context)) {
                column = (Column) var8.next();
                if (isFirst) {
                    isFirst = false;
                } else {
                    createTable.append(", ");
                }
            }

            if (table.getRowId() != null) {
                String rowIdColumn = this.dialect.getRowIdColumnString(table.getRowId());
                if (rowIdColumn != null) {
                    createTable.append(", ").append(rowIdColumn);
                }
            }


            createTable.append(this.dialect.getUniqueDelegate().getTableCreationUniqueConstraintsFragment(table, context));
            this.applyTableCheck(table, createTable);
            createTable.append(')');
            if (table.getComment() != null) {
                createTable.append(this.dialect.getTableComment(table.getComment()));
            }

            this.applyTableTypeString(createTable);
            if (tableEngine != null) {
                createTable.append(tableEngine.name()).append(" (");
                StringJoiner joiner = new StringJoiner(",");
                for (String colId : tableEngine.columns()) {
                    joiner.add(colId);
                }
                createTable.append(joiner);
                createTable.append(") ");
            }
            if (partitionKey != null) {
                createTable.append(" partition by (");
                if (partitionKey.columns().length > 0) {
                    StringJoiner joiner = new StringJoiner(",");
                    for (String colId : partitionKey.columns()) {
                        joiner.add(colId);
                    }
                    createTable.append(joiner);
                }
                createTable.append(") ");
            }

            if (tableEngine != null) {
               if  (!Objects.equals(tableEngine.ttlColumn(), "") && !Objects.equals(tableEngine.ttlDuration(), "")) {
                   createTable.append(" TTL "+ tableEngine.ttlColumn() + " INTERVAL " + tableEngine.ttlDuration());
                   if (!Objects.equals(tableEngine.ttlClause(), "")) {
                       createTable.append(" "+ tableEngine.ttlColumn() + " ");
                   }
               }

            }

            if (table.hasPrimaryKey()) {
                ClickHousePrimaryKey clickHousePrimaryKey = new ClickHousePrimaryKey(table.getPrimaryKey());
                createTable.append(clickHousePrimaryKey.sqlConstraintString(this.dialect));
            }


            List<String> sqlStrings = new ArrayList();
            sqlStrings.add(createTable.toString());
            this.applyComments(table, formattedTableName, sqlStrings);
            this.applyInitCommands(table, sqlStrings, context);
            return sqlStrings.toArray(StringHelper.EMPTY_STRINGS);
        } catch (Exception var10) {
            throw var10;
        }
    }

    @Override
    protected void applyTableTypeString(StringBuilder buf) {
        buf.append(this.dialect.getTableTypeString());
    }

}

