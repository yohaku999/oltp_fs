package dev.yohaku.dbfs.jdbc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

final class SqlTraceSampler {
    private static final String TRACE_FILE_ENV = "DBFS_SQL_TRACE_SAMPLE_FILE";
    private static final Set<String> SEEN_KEYS = ConcurrentHashMap.newKeySet();

    private SqlTraceSampler() {
    }

    static void record(String kind, String shapeKey, Supplier<String> renderedSqlSupplier) {
        String traceFile = System.getenv(TRACE_FILE_ENV);
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
}