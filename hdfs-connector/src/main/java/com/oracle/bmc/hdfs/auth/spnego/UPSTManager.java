package com.oracle.bmc.hdfs.auth.spnego;

import com.oracle.bmc.util.VisibleForTesting;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Manages the generation and retrieval of UPST tokens by leveraging SPNEGO tokens and RSA keys.
 */
public class UPSTManager {

    private final TokenGenerator tokenGenerator;
    private final IAMTokenExchangeClient tokenExchangeClient;
    private final RSAKeyPairGenerator rsaKeyPairGenerator;
    private KeyPair keyPair;

    private UPSTManager(Builder builder) {
        this.tokenGenerator = builder.tokenGenerator;
        this.rsaKeyPairGenerator = builder.rsaKeyPairGenerator;
        this.tokenExchangeClient = builder.tokenExchangeClient;
    }

    public String generateSpnegoToken( ) throws IOException, InterruptedException {
        return tokenGenerator.generateToken();
    }

    private KeyPair generateRSAKeyPair() throws NoSuchAlgorithmException {
        keyPair = rsaKeyPairGenerator.generateKeyPair();
        return keyPair;
    }

    public UPSTResponse getUPSTToken(String spnegoToken) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        if (keyPair == null) {
            generateRSAKeyPair();
        }
        String token = tokenExchangeClient.exchangeToken(spnegoToken, keyPair);
        return new UPSTResponse(token, keyPair.getPrivate(), decodeUPSTAndGetSessionExp(token));
    }

    @VisibleForTesting
    public String decodeUPSTAndGetSessionExp(String token) {
        try {
            String payload = new String(Base64.getDecoder().decode(token.split("\\.")[1]));
            JSONObject payloadJson = new JSONObject(payload);
            return payloadJson.getString("sess_exp");
        } catch (Exception e) {
            throw new RuntimeException("Failed to decode the UPST token and extract sess_exp", e);
        }
    }

    public static class Builder {
        private TokenGenerator tokenGenerator;
        private RSAKeyPairGenerator rsaKeyPairGenerator;
        private IAMTokenExchangeClient tokenExchangeClient;

        public Builder tokenGenerator(TokenGenerator tokenGenerator) {
            this.tokenGenerator = tokenGenerator;
            return this;
        }

        public Builder rsaKeyPairGenerator(RSAKeyPairGenerator rsaKeyPairGenerator) {
            this.rsaKeyPairGenerator = rsaKeyPairGenerator;
            return this;
        }

        public Builder tokenExchangeClient(IAMTokenExchangeClient tokenExchangeClient) {
            this.tokenExchangeClient = tokenExchangeClient;
            return this;
        }

        public UPSTManager build() {
            return new UPSTManager(this);
        }
    }
}
