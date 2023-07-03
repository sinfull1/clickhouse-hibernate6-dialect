package io.hypersistence.utils.hibernate.type.basic;


import io.hypersistence.utils.hibernate.type.MutableType;
import io.hypersistence.utils.hibernate.type.util.Configuration;
import org.hibernate.type.descriptor.jdbc.VarcharJdbcType;
import org.hibernate.type.spi.TypeBootstrapContext;


public class ClickHouseInetType extends MutableType<Inet, VarcharJdbcType, ClickHouseInetVarcharDescriptor> {
    public static final ClickHouseInetType INSTANCE = new ClickHouseInetType();

    public ClickHouseInetType() {
        super(Inet.class, VarcharJdbcType.INSTANCE, ClickHouseInetVarcharDescriptor.INSTANCE);
    }

    public ClickHouseInetType(Configuration configuration) {
        super(Inet.class, VarcharJdbcType.INSTANCE, ClickHouseInetVarcharDescriptor.INSTANCE, configuration);
    }

    public ClickHouseInetType(TypeBootstrapContext typeBootstrapContext) {
        this(new Configuration(typeBootstrapContext.getConfigurationSettings()));
    }

    public String getName() {
        return "IPv4";
    }
}