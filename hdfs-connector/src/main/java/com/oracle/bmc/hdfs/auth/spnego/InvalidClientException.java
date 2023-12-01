package com.oracle.bmc.hdfs.auth.spnego;

/**
 * Exception indicating client authentication failure during token exchange.
 * This exception is thrown when there's a client authentication failure,
 * typically due to incorrect Client ID or Client Secret.
 */
public class InvalidClientException extends RuntimeException {

    public InvalidClientException(String message) {
        super(message);
    }

    public InvalidClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidClientException(Throwable cause) {
        super(cause);
    }
}
