package org.apache.flink.connector.jdbc.dialect;

import org.apache.flink.connector.jdbc.internal.converter.DMRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 达梦实现
 *
 * @author 奔波儿灞
 * @version 1.0.0
 */
public class DMDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    // Define MAX/MIN precision of TIMESTAMP type according to Mysql docs:
    // https://dev.mysql.com/doc/refman/8.0/en/fractional-seconds.html
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 1;

    // Define MAX/MIN precision of DECIMAL type according to Mysql docs:
    // https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html
    private static final int MAX_DECIMAL_PRECISION = 65;
    private static final int MIN_DECIMAL_PRECISION = 1;

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:dm:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new DMRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("dm.jdbc.driver.DmDriver");
    }

    /**
     * DM upsert query use MERGE INTO.
     */
    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String on = Arrays.stream(uniqueKeyFields)
                .map(e -> String.format("A.%s = TMP.%s", quoteIdentifier(e), quoteIdentifier(e)))
                .collect(Collectors.joining(" AND ", "(", ") "));
        String sql = "MERGE INTO " + quoteIdentifier(tableName) + " A USING(SELECT " +
                Arrays.stream(fieldNames).map(e -> ":" + e + " AS " + e).collect(Collectors.joining(", ")) +
                " FROM DUAL) TMP " +
                "ON " + on +
                "WHEN MATCHED THEN " +
                "UPDATE SET " +
                Arrays.stream(fieldNames).filter(name -> {
                    for (String key : uniqueKeyFields) {
                        if (key.equalsIgnoreCase(name)) {
                            return false;
                        }
                    }
                    return true;
                }).map(e -> quoteIdentifier(e) + " = TMP." + quoteIdentifier(e)).collect(Collectors.joining(", ")) +
                " WHEN NOT MATCHED THEN INSERT (" +
                Arrays.stream(fieldNames).map(this::quoteIdentifier).collect(Collectors.joining(", ")) +
                ") VALUES (" +
                Arrays.stream(fieldNames).map(e -> "TMP." + quoteIdentifier(e)).collect(Collectors.joining(", ")) +
                ")";
        return Optional.of(sql);
    }

    @Override
    public String dialectName() {
        return "DM";
    }

    @Override
    public int maxDecimalPrecision() {
        return MAX_DECIMAL_PRECISION;
    }

    @Override
    public int minDecimalPrecision() {
        return MIN_DECIMAL_PRECISION;
    }

    @Override
    public int maxTimestampPrecision() {
        return MAX_TIMESTAMP_PRECISION;
    }

    @Override
    public int minTimestampPrecision() {
        return MIN_TIMESTAMP_PRECISION;
    }

    @Override
    public List<LogicalTypeRoot> unsupportedTypes() {
        // The data types used in Mysql are list at:
        // https://dev.mysql.com/doc/refman/8.0/en/data-types.html

        // TODO: We can't convert BINARY data type to
        //  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in
        // LegacyTypeInfoDataTypeConverter.
        return Arrays.asList(
                LogicalTypeRoot.BINARY,
                LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
                LogicalTypeRoot.INTERVAL_YEAR_MONTH,
                LogicalTypeRoot.INTERVAL_DAY_TIME,
                LogicalTypeRoot.ARRAY,
                LogicalTypeRoot.MULTISET,
                LogicalTypeRoot.MAP,
                LogicalTypeRoot.ROW,
                LogicalTypeRoot.DISTINCT_TYPE,
                LogicalTypeRoot.STRUCTURED_TYPE,
                LogicalTypeRoot.NULL,
                LogicalTypeRoot.RAW,
                LogicalTypeRoot.SYMBOL,
                LogicalTypeRoot.UNRESOLVED);
    }

}
