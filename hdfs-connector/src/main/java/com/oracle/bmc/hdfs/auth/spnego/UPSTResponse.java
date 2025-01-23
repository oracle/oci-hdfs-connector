package com.oracle.bmc.hdfs.auth.spnego;

import com.oracle.bmc.http.client.pki.Pem;

import java.io.ByteArrayOutputStream;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.io.IOException;

public class UPSTResponse {
    private final String upstToken;
    private final PrivateKey privateKey;
    private final String sessionExp;

    public UPSTResponse(String upstToken, PrivateKey privateKey, String sessionExp) {
        this.upstToken = upstToken;
        this.privateKey = privateKey;
        this.sessionExp = sessionExp;
    }

    public String getUpstToken() {
        return upstToken;
    }

    public PrivateKey getPrivateKey() {
        return privateKey;
    }

    public String getSessionExp() {
        return sessionExp;
    }

    public byte[] getPrivateKeyInPEM() {
        if (!(this.privateKey instanceof RSAPrivateKey)) {
            throw new IllegalArgumentException("Expected an RSAPrivateKey instance.");
        }
        return toByteArrayFromRSAPrivateKey((RSAPrivateKey) this.privateKey);
    }

    public static byte[] toByteArrayFromRSAPrivateKey(RSAPrivateKey key) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            Pem.encoder()
                    .with(Pem.Format.LEGACY)
                    .write(java.nio.channels.Channels.newChannel(baos), key);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to encode PEM object", e);
        }
        return baos.toByteArray();
    }

    @Override
    public String toString() {
        return "UPSTResponse{" +
                "upstToken='" + upstToken + '\'' +
                ", privateKey=" + privateKey +
                ", sessionExp='" + sessionExp + '\'' +
                '}';
    }
}
