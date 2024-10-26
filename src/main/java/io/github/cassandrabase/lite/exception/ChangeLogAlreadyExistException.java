package io.github.cassandrabase.lite.exception;

public class ChangeLogAlreadyExistException extends RuntimeException {
    public ChangeLogAlreadyExistException() {
    }

    public ChangeLogAlreadyExistException(Throwable cause) {
        super(cause);
    }

    public ChangeLogAlreadyExistException(String message) {
        super(message);
    }

    public ChangeLogAlreadyExistException(String message, Throwable cause) {
        super(message, cause);
    }

    public ChangeLogAlreadyExistException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
