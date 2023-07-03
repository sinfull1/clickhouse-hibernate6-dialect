package io.hypersistence.utils.hibernate.type.basic;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//


import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.AbstractClassJavaType;

import java.util.Objects;

public class ClickHouseInetVarcharDescriptor extends AbstractClassJavaType<Inet> {
    public static final ClickHouseInetVarcharDescriptor INSTANCE = new ClickHouseInetVarcharDescriptor();

    public ClickHouseInetVarcharDescriptor() {
        super(Inet.class);
    }

    public boolean areEqual(Inet one, Inet another) {
        return Objects.equals(one, another);
    }

    public String toString(Inet value) {
        return value.toString();
    }

    public <X> X unwrap(Inet value, Class<X> type, WrapperOptions options) {
        if (value == null) {
            return null;
        }
        if (String.class.isAssignableFrom(type)) {
            return (X) value.getAddress();
        }
        if (Inet.class.isAssignableFrom(type)) {
            return (X) value;
        }
        throw unknownUnwrap(type);

    }

    @Override
    public <X> Inet wrap(X value, WrapperOptions options) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return new Inet((String) value);
        }
        throw unknownWrap(value.getClass());
    }
}
