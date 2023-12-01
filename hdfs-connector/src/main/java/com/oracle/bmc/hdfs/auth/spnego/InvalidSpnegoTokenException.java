package com.oracle.bmc.hdfs.auth.spnego;

public class InvalidSpnegoTokenException extends RuntimeException {

    public InvalidSpnegoTokenException(String message) {
        super(message);
    }
    public InvalidSpnegoTokenException(String message, Throwable cause) {
        super(message, cause);
    }
}
