package org.hibernate.dialect;

public class ClickHouseStorageEngine implements MySQLStorageEngine {

    @Override
    public boolean supportsCascadeDelete() {
        return true;
    }

    @Override
    public String getTableTypeString(String var1) {
        return " ENGINE = ";
    }

    @Override
    public boolean hasSelfReferentialForeignKeyBug() {
        return false;
    }

    @Override
    public boolean dropConstraints() {
        return false;
    }


}
