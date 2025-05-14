package com.oracle.bmc.hdfs.auth.spnego;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;

public class RSAKeyPairGenerator {

    private final KeyPair keyPair;

    public RSAKeyPairGenerator() {
        this.keyPair = generateNewKeyPair();
    }

    public KeyPair generateKeyPair() {
        return keyPair;
    }

    private KeyPair generateNewKeyPair() {
        try {
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
            keyPairGenerator.initialize(2048);
            return keyPairGenerator.generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Unable to generate RSA KeyPair", e);
        }
    }
}
