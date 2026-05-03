package dev.yohaku.dbfs.jdbc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

final class SqlTraceSampler {
    private static final String TRACE_FILE_ENV = "SQL_TRACE_SAMPLE_FILE";
    private static final String LEGACY_TRACE_FILE_ENV = "DBFS_SQL_TRACE_SAMPLE_FILE";
    private static final String TIMING_TRACE_FILE_ENV = "SQL_TRACE_TIMING_FILE";
    private static final String LEGACY_TIMING_TRACE_FILE_ENV = "DBFS_SQL_TRACE_TIMING_FILE";
    private static final Set<String> SEEN_KEYS = ConcurrentHashMap.newKeySet();
    private static final Map<String, TimingAggregate> TIMING_AGGREGATES = new ConcurrentHashMap<>();
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean(false);

    private SqlTraceSampler() {
    }

    static void record(String kind, String shapeKey, Supplier<String> renderedSqlSupplier) {
        String traceFile = traceFile(TRACE_FILE_ENV, LEGACY_TRACE_FILE_ENV);
        if (traceFile == null || traceFile.isBlank()) {
            return;
        }

        String key = kind + "\n" + shapeKey;
        if (!SEEN_KEYS.add(key)) {
            return;
        }

        synchronized (SqlTraceSampler.class) {
            try {
                Path path = Paths.get(traceFile);
                Path parent = path.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                String renderedSql = renderedSqlSupplier.get();
                Files.writeString(
                        path,
                        "[" + kind + " sample] sql=" + renderedSql + System.lineSeparator(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            } catch (IOException exception) {
                throw new RuntimeException("Failed to write SQL trace sample", exception);
            }
        }
    }

    static void recordTiming(
            String kind,
            String shapeKey,
            Supplier<String> renderedSqlSupplier,
            long elapsedMicros,
            boolean succeeded,
            String errorType) {
        recordTiming(kind, shapeKey, renderedSqlSupplier, elapsedMicros, 1, succeeded, errorType);
    }

    static void recordTiming(
            String kind,
            String shapeKey,
            Supplier<String> renderedSqlSupplier,
            long elapsedMicros,
            long executionCount,
            boolean succeeded,
            String errorType) {
        String traceFile = traceFile(TIMING_TRACE_FILE_ENV, LEGACY_TIMING_TRACE_FILE_ENV);
        if (traceFile == null || traceFile.isBlank()) {
            return;
        }

        registerShutdownHookIfNeeded(traceFile);

        String compactShapeKey = compactSql(shapeKey);
        String aggregateKey = kind + "\n" + compactShapeKey;

        synchronized (SqlTraceSampler.class) {
            TimingAggregate aggregate = TIMING_AGGREGATES.computeIfAbsent(
                    aggregateKey,
                    ignored -> new TimingAggregate(kind, compactShapeKey));
            if (aggregate.sampleSql == null) {
                aggregate.sampleSql = compactSql(renderedSqlSupplier.get());
            }
            aggregate.executions += executionCount;
            aggregate.totalElapsedMicros += elapsedMicros;
            long representativeElapsedMicros = executionCount <= 1
                    ? elapsedMicros
                    : Math.max(1, elapsedMicros / executionCount);
            aggregate.maxElapsedMicros = Math.max(aggregate.maxElapsedMicros, representativeElapsedMicros);
            if (!succeeded) {
                aggregate.failures += 1;
                aggregate.lastErrorType = errorType;
            }
        }
    }

    private static void registerShutdownHookIfNeeded(String traceFile) {
        if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> flushTimingAggregates(traceFile),
                    "dbfs-sql-trace-flush"));
        }
    }

    private static void flushTimingAggregates(String traceFile) {
        synchronized (SqlTraceSampler.class) {
            try {
                Path path = Paths.get(traceFile);
                Path parent = path.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }

                List<TimingAggregate> aggregates = new ArrayList<>(TIMING_AGGREGATES.values());
                aggregates.sort(Comparator.comparingLong((TimingAggregate aggregate) -> aggregate.totalElapsedMicros)
                        .reversed());

                StringBuilder csv = new StringBuilder();
                csv.append("kind,shape_key,sample_sql,executions,total_elapsed_us,mean_elapsed_us,max_elapsed_us,failures,last_error_type")
                        .append(System.lineSeparator());
                for (TimingAggregate aggregate : aggregates) {
                    csv.append(csvField(aggregate.kind)).append(',')
                            .append(csvField(aggregate.shapeKey)).append(',')
                            .append(csvField(aggregate.sampleSql)).append(',')
                            .append(aggregate.executions).append(',')
                            .append(aggregate.totalElapsedMicros).append(',')
                            .append(aggregate.executions == 0 ? 0 : aggregate.totalElapsedMicros / aggregate.executions)
                            .append(',')
                            .append(aggregate.maxElapsedMicros).append(',')
                            .append(aggregate.failures).append(',')
                            .append(csvField(aggregate.lastErrorType))
                            .append(System.lineSeparator());
                }

                Files.writeString(
                        path,
                        csv.toString(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE);
            } catch (IOException exception) {
                throw new RuntimeException("Failed to write SQL timing trace", exception);
            }
        }
    }

    private static String compactSql(String sql) {
        return sql.replaceAll("\\s+", " ").trim();
    }

    private static String traceFile(String primaryEnv, String legacyEnv) {
        String traceFile = System.getenv(primaryEnv);
        if (traceFile != null && !traceFile.isBlank()) {
            return traceFile;
        }
        return System.getenv(legacyEnv);
    }

    private static String csvField(String value) {
        if (value == null) {
            return "";
        }
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private static final class TimingAggregate {
        private final String kind;
        private final String shapeKey;
        private String sampleSql;
        private long executions;
        private long totalElapsedMicros;
        private long maxElapsedMicros;
        private long failures;
        private String lastErrorType;

        private TimingAggregate(String kind, String shapeKey) {
            this.kind = kind;
            this.shapeKey = shapeKey;
        }
    }
}