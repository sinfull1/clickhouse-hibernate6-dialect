//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.hibernate.dialect;


import jakarta.persistence.TemporalType;
import org.hibernate.PessimisticLockException;
import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.boot.model.TypeContributions;
import org.hibernate.dialect.function.*;
import org.hibernate.dialect.hint.IndexQueryHintHandler;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.dialect.identity.MySQLIdentityColumnSupport;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitLimitHandler;
import org.hibernate.dialect.sequence.NoSequenceSupport;
import org.hibernate.dialect.sequence.SequenceSupport;
import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.dialect.temptable.TemporaryTableKind;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.jdbc.env.spi.IdentifierCaseStrategy;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelper;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelperBuilder;
import org.hibernate.engine.jdbc.env.spi.NameQualifierSupport;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.exception.LockTimeoutException;
import org.hibernate.exception.spi.SQLExceptionConversionDelegate;
import org.hibernate.exception.spi.ViolatedConstraintNameExtractor;
import org.hibernate.internal.util.JdbcExceptionHelper;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.query.sqm.CastType;
import org.hibernate.query.sqm.IntervalType;
import org.hibernate.query.sqm.NullOrdering;
import org.hibernate.query.sqm.TemporalUnit;
import org.hibernate.query.sqm.function.SqmFunctionRegistry;
import org.hibernate.query.sqm.mutation.internal.temptable.AfterUseAction;
import org.hibernate.query.sqm.mutation.internal.temptable.BeforeUseAction;
import org.hibernate.query.sqm.mutation.internal.temptable.LocalTemporaryTableInsertStrategy;
import org.hibernate.query.sqm.mutation.internal.temptable.LocalTemporaryTableMutationStrategy;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableInsertStrategy;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategy;
import org.hibernate.query.sqm.produce.function.FunctionParameterType;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.sql.ast.SqlAstNodeRenderingMode;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.spi.StandardSqlAstTranslatorFactory;
import org.hibernate.sql.exec.spi.JdbcOperation;
import org.hibernate.tool.schema.internal.ClickHouseTableExporter;
import org.hibernate.type.BasicTypeRegistry;
import org.hibernate.type.NullType;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.descriptor.DateTimeUtils;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.NullJdbcType;
import org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry;
import org.hibernate.type.descriptor.sql.internal.CapacityDependentDdlType;
import org.hibernate.type.descriptor.sql.internal.DdlTypeImpl;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;

import java.sql.*;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import static org.hibernate.type.SqlTypes.*;

public class ClickHouseDialect extends Dialect {
    private static final DatabaseVersion MINIMUM_VERSION = DatabaseVersion.make(23, 1,2);
    private final ClickHouseStorageEngine storageEngine;
    private final Dialect.SizeStrategy sizeStrategy;
    private final int maxVarcharLength;
    private final int maxVarbinaryLength;
    private final boolean noBackslashEscapesEnabled;

    public ClickHouseDialect() {
        this(MINIMUM_VERSION);
    }

    public ClickHouseDialect(DatabaseVersion version) {
        this(version, 4);
    }

    public ClickHouseDialect(DatabaseVersion version, int bytesPerCharacter) {
        this(version, bytesPerCharacter, false);
    }

    public ClickHouseDialect(DatabaseVersion version, MySQLServerConfiguration serverConfiguration) {
        this(version, serverConfiguration.getBytesPerCharacter(), serverConfiguration.isNoBackslashEscapesEnabled());
    }

    public ClickHouseDialect(DatabaseVersion version, int bytesPerCharacter, boolean noBackslashEscapes) {
        super(version);
        this.storageEngine = this.createStorageEngine();
        this.sizeStrategy = new Dialect.SizeStrategyImpl() {
            public Size resolveSize(JdbcType jdbcType, JavaType<?> javaType, Integer precision, Integer scale, Long length) {
                if (jdbcType.getDdlTypeCode() == -7) {
                    if (length != null) {
                        return Size.length(Math.min(Math.max(length, 1L), 64L));
                    }
                }
                return super.resolveSize(jdbcType, javaType, precision, scale, length);
            }
        };
        this.maxVarcharLength = maxVarcharLength(this.getMySQLVersion(), bytesPerCharacter);
        this.maxVarbinaryLength = maxVarbinaryLength(this.getMySQLVersion());
        this.noBackslashEscapesEnabled = noBackslashEscapes;
    }

    public ClickHouseDialect(DialectResolutionInfo info) {
        this(createVersion(info), MySQLServerConfiguration.fromDatabaseMetadata(info.getDatabaseMetadata()));
        this.registerKeywords(info);
    }

    protected static DatabaseVersion createVersion(DialectResolutionInfo info) {
        String versionString = info.getDatabaseVersion();
        if (versionString != null) {
            String[] components = versionString.split("\\.");
            if (components.length >= 3) {
                try {
                    int majorVersion = Integer.parseInt(components[0]);
                    int minorVersion = Integer.parseInt(components[1]);
                    int patchLevel = Integer.parseInt(components[2]);
                    return DatabaseVersion.make(majorVersion, minorVersion, patchLevel);
                } catch (NumberFormatException var6) {
                    // do nothing
                }
            }
        }

        return info.makeCopy();
    }

    protected DatabaseVersion getMinimumSupportedVersion() {
        return MINIMUM_VERSION;
    }

    protected void initDefaultProperties() {
        super.initDefaultProperties();
        this.getDefaultProperties().setProperty("hibernate.max_fetch_depth", "2");
    }

    private ClickHouseStorageEngine createStorageEngine() {
        return new ClickHouseStorageEngine();
    }


    // Add the correct defintion of the column types
    protected String columnType(int sqlTypeCode) {
        switch (sqlTypeCode) {
            case 2:
                return this.columnType(3);
            case 16:
                return "Bool";
            case 93, 2014, TIME_UTC, TIMESTAMP_UTC, TIME, TIME_WITH_TIMEZONE :
                return "datetime";
            case 2004:
                return "longblob";
            case 2005:
            case 2011:
                return "longtext";
            case DATE:
                return "date";
            default:
                return super.columnType(sqlTypeCode);
        }
    }

    public boolean useMaterializedLobWhenCapacityExceeded() {
        return false;
    }

    protected String castType(int sqlTypeCode) {
        switch (sqlTypeCode) {
            case -15:
            case -9:
            case 1:
            case 12:
            case 4001:
            case 4002:
                return "char";
            case -7:
            case 16:
                return "unsigned";
            case -6:
            case -5:
            case 4:
            case 5:
                return "signed";
            case -3:
            case -2:
            case 4003:
                return "binary";
            case 6:
            case 7:
            case 8:
                return "decimal($p,$s)";
            default:
                return super.castType(sqlTypeCode);
        }
    }

    protected void registerColumnTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
        super.registerColumnTypes(typeContributions, serviceRegistry);
        DdlTypeRegistry ddlTypeRegistry = typeContributions.getTypeConfiguration().getDdlTypeRegistry();
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(3001, "json", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(3200, "geometry", this));
        boolean maxTinyLobLen = true;
        int maxLobLen = '\uffff';
        int maxMediumLobLen = 16777215;
        CapacityDependentDdlType.Builder varcharBuilder = CapacityDependentDdlType.builder(12, this.columnType(2005), "char", this).withTypeCapacity((long) this.getMaxVarcharLength(), "varchar($l)").withTypeCapacity(16777215L, "mediumtext");
        if (this.getMaxVarcharLength() < 65535) {
            varcharBuilder.withTypeCapacity(65535L, "text");
        }

        ddlTypeRegistry.addDescriptor(varcharBuilder.build());
        CapacityDependentDdlType.Builder nvarcharBuilder = CapacityDependentDdlType.builder(-9, this.columnType(2011), "char", this).withTypeCapacity((long) this.getMaxVarcharLength(), "varchar($l)").withTypeCapacity(16777215L, "mediumtext");
        if (this.getMaxVarcharLength() < 65535) {
            nvarcharBuilder.withTypeCapacity(65535L, "text");
        }

        ddlTypeRegistry.addDescriptor(nvarcharBuilder.build());
        CapacityDependentDdlType.Builder varbinaryBuilder = CapacityDependentDdlType.builder(-3, this.columnType(2004), "binary", this).withTypeCapacity((long) this.getMaxVarbinaryLength(), "varbinary($l)").withTypeCapacity(16777215L, "mediumblob");
        if (this.getMaxVarbinaryLength() < 65535) {
            varbinaryBuilder.withTypeCapacity(65535L, "blob");
        }

        ddlTypeRegistry.addDescriptor(varbinaryBuilder.build());
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(4003, this.columnType(2004), "binary", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(4001, this.columnType(2005), "char", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(4002, this.columnType(2005), "char", this));
        ddlTypeRegistry.addDescriptor(CapacityDependentDdlType.builder(2004, this.columnType(2004), "binary", this).withTypeCapacity(255L, "tinyblob").withTypeCapacity(16777215L, "mediumblob").withTypeCapacity(65535L, "blob").build());
        ddlTypeRegistry.addDescriptor(CapacityDependentDdlType.builder(2005, this.columnType(2005), "char", this).withTypeCapacity(255L, "tinytext").withTypeCapacity(16777215L, "mediumtext").withTypeCapacity(65535L, "text").build());
        ddlTypeRegistry.addDescriptor(CapacityDependentDdlType.builder(2011, this.columnType(2011), "char", this).withTypeCapacity(255L, "tinytext").withTypeCapacity(16777215L, "mediumtext").withTypeCapacity(65535L, "text").build());
    }

    /**
     * @deprecated
     */
    @Deprecated
    protected static int getCharacterSetBytesPerCharacter(DatabaseMetaData databaseMetaData) {
        if (databaseMetaData != null) {
            try {
                Statement s = databaseMetaData.getConnection().createStatement();

                byte var7;
                label139:
                {
                    label140:
                    {
                        label141:
                        {
                            label151:
                            {
                                try {
                                    ResultSet rs = s.executeQuery("SELECT @@character_set_database");
                                    if (rs.next()) {
                                        String characterSet = rs.getString(1);
                                        int collationIndex = characterSet.indexOf(95);
                                        switch (collationIndex == -1 ? characterSet : characterSet.substring(0, collationIndex)) {
                                            case "utf16":
                                            case "utf16le":
                                            case "utf32":
                                            case "utf8mb4":
                                            case "gb18030":
                                                var7 = 4;
                                                break label151;
                                            case "utf8":
                                            case "utf8mb3":
                                            case "eucjpms":
                                            case "ujis":
                                                var7 = 3;
                                                break label141;
                                            case "ucs2":
                                            case "cp932":
                                            case "big5":
                                            case "euckr":
                                            case "gb2312":
                                            case "gbk":
                                            case "sjis":
                                                var7 = 2;
                                                break label140;
                                            default:
                                                var7 = 1;
                                                break label139;
                                        }
                                    }
                                } catch (Throwable var9) {
                                    if (s != null) {
                                        try {
                                            s.close();
                                        } catch (Throwable var8) {
                                            var9.addSuppressed(var8);
                                        }
                                    }

                                    throw var9;
                                }

                                if (s != null) {
                                    s.close();
                                }

                                return 4;
                            }

                            if (s != null) {
                                s.close();
                            }

                            return var7;
                        }

                        if (s != null) {
                            s.close();
                        }

                        return var7;
                    }

                    if (s != null) {
                        s.close();
                    }

                    return var7;
                }

                if (s != null) {
                    s.close();
                }

                return var7;
            } catch (SQLException var10) {
            }
        }

        return 4;
    }

    private static int maxVarbinaryLength(DatabaseVersion version) {
        return 65535;
    }

    private static int maxVarcharLength(DatabaseVersion version, int bytesPerCharacter) {
        switch (bytesPerCharacter) {
            case 1:
                return 65535;
            case 2:
                return 32767;
            case 3:
                return 21844;
            case 4:
            default:
                return 16383;
        }
    }

    public int getMaxVarcharLength() {
        return this.maxVarcharLength;
    }

    public int getMaxVarbinaryLength() {
        return this.maxVarbinaryLength;
    }

    public boolean isNoBackslashEscapesEnabled() {
        return this.noBackslashEscapesEnabled;
    }

    public String getNullColumnString(String columnType) {
        return " Nullable(" +columnType+") ";
    }

    public DatabaseVersion getMySQLVersion() {
        return super.getVersion();
    }

    public Dialect.SizeStrategy getSizeStrategy() {
        return this.sizeStrategy;
    }

    public long getDefaultLobLength() {
        return 16777215L;
    }

    public JdbcType resolveSqlTypeDescriptor(String columnTypeName, int jdbcTypeCode, int precision, int scale, JdbcTypeRegistry jdbcTypeRegistry) {
        switch (jdbcTypeCode) {
            case -7:
                return jdbcTypeRegistry.getDescriptor(16);
            case -2:
                if ("GEOMETRY".equals(columnTypeName)) {
                    jdbcTypeCode = 3200;
                }
            default:
                return super.resolveSqlTypeDescriptor(columnTypeName, jdbcTypeCode, precision, scale, jdbcTypeRegistry);
        }
    }

    public int getPreferredSqlTypeCodeForBoolean() {
        return -7;
    }

    public void initializeFunctionRegistry(FunctionContributions functionContributions) {
        super.initializeFunctionRegistry(functionContributions);
        CommonFunctionFactory functionFactory = new CommonFunctionFactory(functionContributions);
        functionFactory.soundex();
        functionFactory.radians();
        functionFactory.degrees();
        functionFactory.cot();
        functionFactory.log();
        functionFactory.log2();
        functionFactory.log10();
        functionFactory.trim2();
        functionFactory.octetLength();
        functionFactory.reverse();
        functionFactory.space();
        functionFactory.repeat();
        functionFactory.pad_space();
        functionFactory.md5();
        functionFactory.yearMonthDay();
        functionFactory.hourMinuteSecond();
        functionFactory.dayofweekmonthyear();
        functionFactory.weekQuarter();
        functionFactory.daynameMonthname();
        functionFactory.lastDay();
        functionFactory.date();
        functionFactory.timestamp();
        this.time(functionContributions);
        functionFactory.utcDateTimeTimestamp();
        functionFactory.rand();
        functionFactory.crc32();
        functionFactory.sha1();
        functionFactory.sha2();
        functionFactory.sha();
        functionFactory.bitLength();
        functionFactory.octetLength();
        functionFactory.ascii();
        functionFactory.instr();
        functionFactory.substr();
        functionFactory.position();
        functionFactory.nowCurdateCurtime();
        functionFactory.trunc_truncate();
        functionFactory.insert();
        functionFactory.bitandorxornot_operator();
        functionFactory.bitAndOr();
        functionFactory.stddev();
        functionFactory.stddevPopSamp();
        functionFactory.variance();
        functionFactory.varPopSamp();
        functionFactory.datediff();
        functionFactory.adddateSubdateAddtimeSubtime();
        functionFactory.format_dateFormat();
        functionFactory.makedateMaketime();
        functionFactory.localtimeLocaltimestamp();
        BasicTypeRegistry basicTypeRegistry = functionContributions.getTypeConfiguration().getBasicTypeRegistry();
        SqmFunctionRegistry functionRegistry = functionContributions.getFunctionRegistry();
        functionRegistry.register(
                "arraySort",
                new ArraySortFunction(
                        this,
                        functionContributions.getTypeConfiguration(),
                        SqlAstNodeRenderingMode.DEFAULT
                )
        );
        functionRegistry.register(
                "length",
                new ArrayLengthFunction(
                        this,
                        functionContributions.getTypeConfiguration(),
                        SqlAstNodeRenderingMode.DEFAULT
                )
        );

        functionRegistry.register(
                "arrayAvg",
                new ArrayAvgFunction(
                        this,
                        functionContributions.getTypeConfiguration(),
                        SqlAstNodeRenderingMode.DEFAULT
                )
        );
        functionRegistry.register(
                "arraySum",
                new ArraySumFunction(
                        this,
                        functionContributions.getTypeConfiguration(),
                        SqlAstNodeRenderingMode.DEFAULT
                )
        );
        functionRegistry.register(
                "topK",
                new TopKFunction(
                        this,
                        functionContributions.getTypeConfiguration(),
                        SqlAstNodeRenderingMode.DEFAULT
                )
        );
        functionRegistry.register(
                "groupArray",
                new GroupArrayFunction(
                        this,
                        functionContributions.getTypeConfiguration(),
                        SqlAstNodeRenderingMode.DEFAULT
                )
        );
        functionRegistry.register(
                "groupArray",
                new GroupArrayFunction(
                        this,
                        functionContributions.getTypeConfiguration(),
                        SqlAstNodeRenderingMode.DEFAULT
                )
        );

        functionRegistry.noArgsBuilder("localtime").setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.TIMESTAMP)).setUseParenthesesWhenNoArgs(false).register();
        if (this.getMySQLVersion().isSameOrAfter(8)) {
            functionRegistry.patternDescriptorBuilder("pi", "cast(pi() as double)").setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.DOUBLE)).setExactArgumentCount(0).setArgumentListSignature("").register();
        } else {
            functionRegistry.patternDescriptorBuilder("pi", "cast(pi() as decimal(53,15))").setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.DOUBLE)).setExactArgumentCount(0).setArgumentListSignature("").register();
        }

        functionRegistry.patternDescriptorBuilder("chr", "char(?1 using ascii)").setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.CHARACTER)).setExactArgumentCount(1).setParameterTypes(new FunctionParameterType[]{FunctionParameterType.INTEGER}).register();
        functionRegistry.registerAlternateKey("char", "chr");
        functionFactory.sysdateExplicitMicros();
        if (this.getMySQLVersion().isSameOrAfter(8, 0, 2)) {
            functionFactory.windowFunctions();
            if (this.getMySQLVersion().isSameOrAfter(8, 0, 11)) {
                functionFactory.hypotheticalOrderedSetAggregates_windowEmulation();
            }
        }

        functionFactory.listagg_groupConcat();
    }

    public void contributeTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
        super.contributeTypes(typeContributions, serviceRegistry);
        JdbcTypeRegistry jdbcTypeRegistry = typeContributions.getTypeConfiguration().getJdbcTypeRegistry();
        jdbcTypeRegistry.addDescriptorIfAbsent(3001, MySQLCastingJsonJdbcType.INSTANCE);
        typeContributions.contributeJdbcType(NullJdbcType.INSTANCE);
        typeContributions.contributeType(new NullType(NullJdbcType.INSTANCE, typeContributions.getTypeConfiguration().getJavaTypeRegistry().getDescriptor(Object.class)));
    }

    public SqlAstTranslatorFactory getSqlAstTranslatorFactory() {
        return new StandardSqlAstTranslatorFactory() {
            protected <T extends JdbcOperation> SqlAstTranslator<T> buildTranslator(SessionFactoryImplementor sessionFactory, org.hibernate.sql.ast.tree.Statement statement) {
                return new ClickHouseSqlAstTranslator(sessionFactory, statement);
            }
        };
    }

    public String castPattern(CastType from, CastType to) {
        if (to == CastType.INTEGER_BOOLEAN) {
            switch (from) {
                case STRING:
                case INTEGER:
                case LONG:
                case YN_BOOLEAN:
                case TF_BOOLEAN:
                case BOOLEAN:
                    break;
                default:
                    return "abs(sign(?1))";
            }
        }

        return super.castPattern(from, to);
    }

    private void time(FunctionContributions queryEngine) {
        queryEngine.getFunctionRegistry().namedDescriptorBuilder("time").setExactArgumentCount(1).setInvariantType(queryEngine.getTypeConfiguration().getBasicTypeRegistry().resolve(StandardBasicTypes.STRING)).register();
    }

    public int getFloatPrecision() {
        return 23;
    }

    public String currentTimestamp() {
        return "current_timestamp(6)";
    }

    public long getFractionalSecondPrecisionInNanos() {
        return 1000L;
    }

    public String extractPattern(TemporalUnit unit) {
        switch (unit) {
            case SECOND:
                return "(second(?2)+microsecond(?2)/1e6)";
            case WEEK:
                return "weekofyear(?2)";
            case DAY_OF_WEEK:
                return "dayofweek(?2)";
            case DAY_OF_MONTH:
                return "dayofmonth(?2)";
            case DAY_OF_YEAR:
                return "dayofyear(?2)";
            case EPOCH:
                return "unix_timestamp(?2)";
            default:
                return "?1(?2)";
        }
    }

    public String timestampaddPattern(TemporalUnit unit, TemporalType temporalType, IntervalType intervalType) {
        switch (unit) {
            case NANOSECOND:
                return "timestampadd(microsecond,(?2)/1e3,?3)";
            case NATIVE:
                return "timestampadd(microsecond,?2,?3)";
            default:
                return "timestampadd(?1,?2,?3)";
        }
    }

    public String timestampdiffPattern(TemporalUnit unit, TemporalType fromTemporalType, TemporalType toTemporalType) {
        switch (unit) {
            case NANOSECOND:
                return "timestampdiff(microsecond,?2,?3)*1e3";
            case NATIVE:
                return "timestampdiff(microsecond,?2,?3)";
            default:
                return "timestampdiff(?1,?2,?3)";
        }
    }

    public boolean supportsTemporalLiteralOffset() {
        return this.getMySQLVersion().isSameOrAfter(8, 0, 19);
    }

    public void appendDateTimeLiteral(SqlAppender appender, TemporalAccessor temporalAccessor, TemporalType precision, TimeZone jdbcTimeZone) {
        switch (precision) {
            case DATE:
                appender.appendSql("date '");
                DateTimeUtils.appendAsDate(appender, (TemporalAccessor) temporalAccessor);
                appender.appendSql('\'');
                break;
            case TIME:
                appender.appendSql("time '");
                DateTimeUtils.appendAsLocalTime(appender, (TemporalAccessor) temporalAccessor);
                appender.appendSql('\'');
                break;
            case TIMESTAMP:
                if (temporalAccessor instanceof ZonedDateTime) {
                    temporalAccessor = ((ZonedDateTime) temporalAccessor).toOffsetDateTime();
                }

                appender.appendSql("timestamp '");
                DateTimeUtils.appendAsTimestampWithMicros(appender, (TemporalAccessor) temporalAccessor, this.supportsTemporalLiteralOffset(), jdbcTimeZone, false);
                appender.appendSql('\'');
                break;
            default:
                throw new IllegalArgumentException();
        }

    }

    public void appendDateTimeLiteral(SqlAppender appender, Date date, TemporalType precision, TimeZone jdbcTimeZone) {
        switch (precision) {
            case DATE:
                appender.appendSql("date '");
                DateTimeUtils.appendAsDate(appender, date);
                appender.appendSql('\'');
                break;
            case TIME:
                appender.appendSql("time '");
                DateTimeUtils.appendAsLocalTime(appender, date);
                appender.appendSql('\'');
                break;
            case TIMESTAMP:
                appender.appendSql("timestamp '");
                DateTimeUtils.appendAsTimestampWithMicros(appender, date, jdbcTimeZone);
                appender.appendSql('\'');
                break;
            default:
                throw new IllegalArgumentException();
        }

    }

    public void appendDateTimeLiteral(SqlAppender appender, Calendar calendar, TemporalType precision, TimeZone jdbcTimeZone) {
        switch (precision) {
            case DATE:
                appender.appendSql("date '");
                DateTimeUtils.appendAsDate(appender, calendar);
                appender.appendSql('\'');
                break;
            case TIME:
                appender.appendSql("time '");
                DateTimeUtils.appendAsLocalTime(appender, calendar);
                appender.appendSql('\'');
                break;
            case TIMESTAMP:
                appender.appendSql("timestamp '");
                DateTimeUtils.appendAsTimestampWithMillis(appender, calendar, jdbcTimeZone);
                appender.appendSql('\'');
                break;
            default:
                throw new IllegalArgumentException();
        }

    }

    public SelectItemReferenceStrategy getGroupBySelectItemReferenceStrategy() {
        return SelectItemReferenceStrategy.POSITION;
    }

    public boolean supportsColumnCheck() {
        return this.getMySQLVersion().isSameOrAfter(8, 0, 16);
    }

    public String getEnumTypeDeclaration(String[] values) {
        StringBuilder type = new StringBuilder();
        type.append("enum (");
        String separator = "";
        String[] var4 = values;
        int var5 = values.length;

        for (int var6 = 0; var6 < var5; ++var6) {
            String value = var4[var6];
            type.append(separator).append('\'').append(value).append('\'');
            separator = ",";
        }

        return type.append(')').toString();
    }

    public String getCheckCondition(String columnName, String[] values) {
        return null;
    }

    public String getQueryHintString(String query, String hints) {
        return IndexQueryHintHandler.INSTANCE.addQueryHints(query, hints);
    }

    public SequenceSupport getSequenceSupport() {
        return NoSequenceSupport.INSTANCE;
    }

    //TODO
    public ViolatedConstraintNameExtractor getViolatedConstraintNameExtractor() {
        return null;//ViolatedConstraintNameExtractor;
    }

    public boolean qualifyIndexName() {
        return false;
    }

    public String getAddForeignKeyConstraintString(String constraintName, String[] foreignKey, String referencedTable, String[] primaryKey, boolean referencesPrimaryKey) {
        String cols = String.join(", ", foreignKey);
        String referencedCols = String.join(", ", primaryKey);
        return String.format(" add constraint %s foreign key (%s) references %s (%s)", constraintName, cols, referencedTable, referencedCols);
    }

    public String getDropForeignKeyString() {
        return "drop foreign key";
    }

    public String getDropUniqueKeyString() {
        return "drop index";
    }

    public String getAlterColumnTypeString(String columnName, String columnType, String columnDefinition) {
        return "modify column " + columnName + " " + columnDefinition;
    }

    public boolean supportsAlterColumnType() {
        return true;
    }

    public LimitHandler getLimitHandler() {
        return LimitLimitHandler.INSTANCE;
    }

    public char closeQuote() {
        return '`';
    }

    public char openQuote() {
        return '`';
    }

    public boolean canCreateCatalog() {
        return true;
    }

    public String[] getCreateCatalogCommand(String catalogName) {
        return new String[]{"create database " + catalogName};
    }

    public String[] getDropCatalogCommand(String catalogName) {
        return new String[]{"drop database " + catalogName};
    }

    public boolean canCreateSchema() {
        return false;
    }

    public String[] getCreateSchemaCommand(String schemaName) {
        throw new UnsupportedOperationException("MySQL does not support dropping creating/dropping schemas in the JDBC sense");
    }

    public String[] getDropSchemaCommand(String schemaName) {
        throw new UnsupportedOperationException("MySQL does not support dropping creating/dropping schemas in the JDBC sense");
    }

    public boolean supportsIfExistsBeforeTableName() {
        return true;
    }

    public String getSelectGUIDString() {
        return "select uuid()";
    }

    @Override
    public ClickHouseTableExporter getTableExporter() {
        return new ClickHouseTableExporter(this);
    }

    public String getTableComment(String comment) {
        return " comment='" + comment + "'";
    }

    public String getColumnComment(String comment) {
        return " comment '" + comment + "'";
    }

    public NullOrdering getNullOrdering() {
        return NullOrdering.SMALLEST;
    }

    public SqmMultiTableMutationStrategy getFallbackSqmMutationStrategy(EntityMappingType rootEntityDescriptor, RuntimeModelCreationContext runtimeModelCreationContext) {
        return new LocalTemporaryTableMutationStrategy(TemporaryTable.createIdTable(rootEntityDescriptor, (basename) -> {
            return "HT_" + basename;
        }, this, runtimeModelCreationContext), runtimeModelCreationContext.getSessionFactory());
    }

    public SqmMultiTableInsertStrategy getFallbackSqmInsertStrategy(EntityMappingType rootEntityDescriptor, RuntimeModelCreationContext runtimeModelCreationContext) {
        return new LocalTemporaryTableInsertStrategy(TemporaryTable.createEntityTable(rootEntityDescriptor, (name) -> {
            return "HTE_" + name;
        }, this, runtimeModelCreationContext), runtimeModelCreationContext.getSessionFactory());
    }

    public TemporaryTableKind getSupportedTemporaryTableKind() {
        return TemporaryTableKind.LOCAL;
    }

    public String getTemporaryTableCreateCommand() {
        return "create temporary table if not exists";
    }

    public String getTemporaryTableDropCommand() {
        return "drop temporary table";
    }

    public AfterUseAction getTemporaryTableAfterUseAction() {
        return AfterUseAction.DROP;
    }

    public BeforeUseAction getTemporaryTableBeforeUseAction() {
        return BeforeUseAction.CREATE;
    }

    public int getMaxAliasLength() {
        return 246;
    }

    public int getMaxIdentifierLength() {
        return 64;
    }

    public boolean supportsCurrentTimestampSelection() {
        return true;
    }

    public boolean isCurrentTimestampSelectStringCallable() {
        return false;
    }

    public String getCurrentTimestampSelectString() {
        return "select now()";
    }

    public int registerResultSetOutParameter(CallableStatement statement, int col) throws SQLException {
        return col;
    }

    public ResultSet getResultSet(CallableStatement ps) throws SQLException {
        for (boolean isResultSet = ps.execute(); !isResultSet && ps.getUpdateCount() != -1; isResultSet = ps.getMoreResults()) {
        }

        return ps.getResultSet();
    }

    public boolean supportsNullPrecedence() {
        return false;
    }

    public boolean supportsLobValueChangePropagation() {
        return false;
    }

    public boolean supportsSubqueryOnMutatingTable() {
        return false;
    }

    public boolean supportsLockTimeouts() {
        return false;
    }

    public SQLExceptionConversionDelegate buildSQLExceptionConversionDelegate() {
        return (sqlException, message, sql) -> {
            switch (sqlException.getErrorCode()) {
                case 1205:
                case 3572:
                    return new PessimisticLockException(message, sqlException, sql);
                case 1206:
                case 1207:
                    return new LockAcquisitionException(message, sqlException, sql);
                default:
                    String sqlState = JdbcExceptionHelper.extractSqlState(sqlException);
                    if (sqlState != null) {
                        switch (sqlState) {
                            case "41000":
                                return new LockTimeoutException(message, sqlException, sql);
                            case "40001":
                                return new LockAcquisitionException(message, sqlException, sql);
                        }
                    }

                    return null;
            }
        };
    }

    public NameQualifierSupport getNameQualifierSupport() {
        return NameQualifierSupport.CATALOG;
    }

    public IdentifierHelper buildIdentifierHelper(IdentifierHelperBuilder builder, DatabaseMetaData dbMetaData) throws SQLException {
        if (dbMetaData == null) {
            builder.setUnquotedCaseStrategy(IdentifierCaseStrategy.MIXED);
            builder.setQuotedCaseStrategy(IdentifierCaseStrategy.MIXED);
        }

        return super.buildIdentifierHelper(builder, dbMetaData);
    }

    public IdentityColumnSupport getIdentityColumnSupport() {
        return MySQLIdentityColumnSupport.INSTANCE;
    }

    public boolean isJdbcLogWarningsEnabledByDefault() {
        return false;
    }

    public boolean supportsCascadeDelete() {
        return this.storageEngine.supportsCascadeDelete();
    }

    public String getTableTypeString() {
        return this.storageEngine.getTableTypeString("engine");
    }

    public boolean hasSelfReferentialForeignKeyBug() {
        return this.storageEngine.hasSelfReferentialForeignKeyBug();
    }

    public boolean dropConstraints() {
        return this.storageEngine.dropConstraints();
    }


    public void appendLiteral(SqlAppender appender, String literal) {
        appender.appendSql('\'');

        for (int i = 0; i < literal.length(); ++i) {
            char c = literal.charAt(i);
            switch (c) {
                case '\'':
                    appender.appendSql('\'');
                    break;
                case '\\':
                    if (!this.noBackslashEscapesEnabled) {
                        appender.appendSql('\\');
                    }
            }

            appender.appendSql(c);
        }

        appender.appendSql('\'');
    }

    public void appendDatetimeFormat(SqlAppender appender, String format) {
        appender.appendSql(datetimeFormat(format).result());
    }

    public static Replacer datetimeFormat(String format) {
        return (new Replacer(format, "'", "")).replace("%", "%%").replace("yyyy", "%Y").replace("yyy", "%Y").replace("yy", "%y").replace("y", "%Y").replace("MMMM", "%M").replace("MMM", "%b").replace("MM", "%m").replace("M", "%c").replace("ww", "%v").replace("w", "%v").replace("YYYY", "%x").replace("YYY", "%x").replace("YY", "%x").replace("Y", "%x").replace("EEEE", "%W").replace("EEE", "%a").replace("ee", "%w").replace("e", "%w").replace("dd", "%d").replace("d", "%e").replace("DDD", "%j").replace("DD", "%j").replace("D", "%j").replace("a", "%p").replace("hh", "%I").replace("HH", "%H").replace("h", "%l").replace("H", "%k").replace("mm", "%i").replace("m", "%i").replace("ss", "%S").replace("s", "%S").replace("SSSSSS", "%f").replace("SSSSS", "%f").replace("SSSS", "%f").replace("SSS", "%f").replace("SS", "%f").replace("S", "%f");
    }

    private String withTimeout(String lockString, int timeout) {
        switch (timeout) {
            case -2:
                return this.supportsSkipLocked() ? lockString + " skip locked" : lockString;
            case -1:
                return lockString;
            case 0:
                return this.supportsNoWait() ? lockString + " nowait" : lockString;
            default:
                return this.supportsWait() ? lockString + " wait " + timeout : lockString;
        }
    }

    public String getWriteLockString(int timeout) {
        return this.withTimeout(this.getForUpdateString(), timeout);
    }

    public String getWriteLockString(String aliases, int timeout) {
        return this.withTimeout(this.getForUpdateString(aliases), timeout);
    }

    public String getReadLockString(int timeout) {
        return this.withTimeout(this.supportsForShare() ? " for share" : " lock in share mode", timeout);
    }

    public String getReadLockString(String aliases, int timeout) {
        return this.supportsAliasLocks() && this.supportsForShare() ? this.withTimeout(" for share of " + aliases, timeout) : this.getReadLockString(timeout);
    }

    public String getForUpdateSkipLockedString() {
        return this.supportsSkipLocked() ? " for update skip locked" : this.getForUpdateString();
    }

    public String getForUpdateSkipLockedString(String aliases) {
        return this.supportsSkipLocked() && this.supportsAliasLocks() ? this.getForUpdateString(aliases) + " skip locked" : this.getForUpdateSkipLockedString();
    }

    public String getForUpdateNowaitString() {
        return this.supportsNoWait() ? " for update nowait" : this.getForUpdateString();
    }

    public String getForUpdateNowaitString(String aliases) {
        return this.supportsNoWait() && this.supportsAliasLocks() ? this.getForUpdateString(aliases) + " nowait" : this.getForUpdateNowaitString();
    }

    public String getForUpdateString(String aliases) {
        return this.supportsAliasLocks() ? " for update of " + aliases : this.getForUpdateString();
    }

    public boolean supportsOffsetInSubquery() {
        return true;
    }

    public boolean supportsWindowFunctions() {
        return this.getMySQLVersion().isSameOrAfter(8, 0, 2);
    }

    public boolean supportsLateral() {
        return this.getMySQLVersion().isSameOrAfter(8, 0, 14);
    }

    public boolean supportsRecursiveCTE() {
        return this.getMySQLVersion().isSameOrAfter(8, 0, 14);
    }

    public boolean supportsSkipLocked() {
        return this.getMySQLVersion().isSameOrAfter(8);
    }

    public boolean supportsNoWait() {
        return this.getMySQLVersion().isSameOrAfter(8);
    }

    public boolean supportsWait() {
        return false;
    }

    public RowLockStrategy getWriteRowLockStrategy() {
        return this.supportsAliasLocks() ? RowLockStrategy.TABLE : RowLockStrategy.NONE;
    }

    protected void registerDefaultKeywords() {
        super.registerDefaultKeywords();
        this.registerKeyword("key");
    }

    boolean supportsForShare() {
        return this.getMySQLVersion().isSameOrAfter(8);
    }

    boolean supportsAliasLocks() {
        return this.getMySQLVersion().isSameOrAfter(8);
    }

    public boolean canDisableConstraints() {
        return true;
    }

    public String getDisableConstraintsStatement() {
        return "set foreign_key_checks = 0";
    }

    public String getEnableConstraintsStatement() {
        return "set foreign_key_checks = 1";
    }
}
