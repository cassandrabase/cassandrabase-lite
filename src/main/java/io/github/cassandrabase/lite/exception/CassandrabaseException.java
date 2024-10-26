package io.github.cassandrabase.lite.exception;

public class CassandrabaseException extends Exception{
    public CassandrabaseException() {
    }

    public CassandrabaseException(Throwable cause) {
        super(cause);
    }

    public CassandrabaseException(String message) {
        super(message);
    }

    public CassandrabaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public CassandrabaseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
