package com.oracle.bmc.hdfs.auth.spnego;


import java.io.UnsupportedEncodingException;
import java.security.KeyPair;

/**
 * Interface representing a service for exchanging tokens.
 */
public interface TokenExchangeService {

    /**
     * Exchanges a specific token (e.g., SPNEGO) for an IAM token.
     *
     * @param token   The token to be exchanged.
     * @param keyPair The key pair used for signing the request.
     * @return The IAM token obtained from the exchange, or null if an error occurred.
     */
    String exchangeToken(String token, KeyPair keyPair) throws UnsupportedEncodingException;
}
