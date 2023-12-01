package com.oracle.bmc.hdfs.auth.spnego;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.interfaces.RSAPrivateKey;

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
        try (JcaPEMWriter writer =
                     new JcaPEMWriter(new OutputStreamWriter(baos, StandardCharsets.UTF_8))) {
            writer.writeObject(key);
            writer.flush();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write PEM object", e);
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
