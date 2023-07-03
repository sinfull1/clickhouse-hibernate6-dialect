package org.hibernate.mapping;

import org.hibernate.dialect.Dialect;

import java.util.Iterator;


public class ClickHousePrimaryKey {

    final PrimaryKey primaryKey;

    public ClickHousePrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }


    public String sqlConstraintString(Dialect dialect) {
        StringBuilder buf = new StringBuilder(" order by ( ");
        Iterator iter = primaryKey.getColumnIterator();

        while (iter.hasNext()) {
            buf.append(((Column) iter.next()).getQuotedName(dialect));
            if (iter.hasNext()) {
                buf.append(", ");
            }
        }

        return buf.append(')').toString();
    }

}
