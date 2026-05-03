package dev.yohaku.dbfs.jdbc;

import java.sql.Timestamp;
import java.util.List;

final class SqlLiteralRenderer {
    private SqlLiteralRenderer() {
    }

    static String render(String sql, List<Object> parameters) {
        if (parameters.isEmpty()) {
            return sql;
        }

        StringBuilder rendered = new StringBuilder(sql.length() + parameters.size() * 8);
        int parameterIndex = 0;
        boolean inSingleQuotedString = false;
        for (int index = 0; index < sql.length(); index += 1) {
            char current = sql.charAt(index);
            if (current == '\'') {
                rendered.append(current);
                if (inSingleQuotedString && index + 1 < sql.length() && sql.charAt(index + 1) == '\'') {
                    rendered.append(sql.charAt(index + 1));
                    index += 1;
                    continue;
                }
                inSingleQuotedString = !inSingleQuotedString;
                continue;
            }

            if (current == '?' && !inSingleQuotedString && parameterIndex < parameters.size()) {
                rendered.append(renderLiteral(parameters.get(parameterIndex)));
                parameterIndex += 1;
                continue;
            }

            rendered.append(current);
        }
        return rendered.toString();
    }

    private static String renderLiteral(Object parameter) {
        if (parameter == null) {
            return "NULL";
        }
        if (parameter instanceof String) {
            return quote((String) parameter);
        }
        if (parameter instanceof Character) {
            return quote(parameter.toString());
        }
        if (parameter instanceof Timestamp) {
            return quote(parameter.toString());
        }
        if (parameter instanceof java.util.Date) {
            return quote(new Timestamp(((java.util.Date) parameter).getTime()).toString());
        }
        if (parameter instanceof Boolean) {
            return (Boolean) parameter ? "TRUE" : "FALSE";
        }
        if (parameter instanceof Number) {
            return parameter.toString();
        }
        return quote(parameter.toString());
    }

    private static String quote(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}