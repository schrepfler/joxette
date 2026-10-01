package com.joxette.support;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Wraps a {@link Connection} and records every SQL string issued through it — the
 * argument to {@code prepareStatement}/{@code prepareCall}, and to
 * {@code execute}/{@code executeQuery}/{@code executeUpdate}/{@code addBatch} on a
 * statement from {@code createStatement}. Lets a test prove which connection a piece of
 * SQL ran on (e.g. that S3-heavy maintenance never touches the shared connection).
 *
 * <p>The proxy is not a {@code DuckDBConnection}, so code that unwraps or casts the
 * connection or its statements to DuckDB types cannot be given one.
 */
public final class SqlRecordingConnection {

    private final Connection proxy;
    private final List<String> sql = new CopyOnWriteArrayList<>();

    private SqlRecordingConnection(Connection target) {
        this.proxy = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                recording(target, (method, args) -> {
                    String name = method.getName();
                    if ((name.equals("prepareStatement") || name.equals("prepareCall"))
                            && args != null && args[0] instanceof String s) {
                        sql.add(s);
                    }
                }, true));
    }

    public static SqlRecordingConnection wrap(Connection target) {
        return new SqlRecordingConnection(target);
    }

    public Connection connection() { return proxy; }

    /** Every SQL string seen so far, in issue order. */
    public List<String> sql() { return List.copyOf(sql); }

    private interface Recorder { void record(java.lang.reflect.Method m, Object[] args); }

    private InvocationHandler recording(Object target, Recorder recorder, boolean isConnection) {
        return (p, method, args) -> {
            recorder.record(method, args);
            Object result;
            try {
                result = method.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            if (isConnection && method.getName().equals("createStatement") && result instanceof Statement st) {
                return Proxy.newProxyInstance(Statement.class.getClassLoader(), new Class<?>[]{Statement.class},
                        recording(st, (m, a) -> {
                            if (m.getName().matches("execute|executeQuery|executeUpdate|executeLargeUpdate|addBatch")
                                    && a != null && a[0] instanceof String s) {
                                sql.add(s);
                            }
                        }, false));
            }
            return result;
        };
    }
}
