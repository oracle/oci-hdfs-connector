package com.oracle.bmc.hdfs.auth.spnego;

import org.junit.Test;

import java.security.KeyPair;

import static org.junit.Assert.*;

public class RSAKeyPairGeneratorTest {

    @Test
    public void testSameKeyPairReturned() {
        RSAKeyPairGenerator generator = new RSAKeyPairGenerator();

        KeyPair first = generator.generateKeyPair();
        KeyPair second = generator.generateKeyPair();

        assertNotNull(first);
        assertNotNull(second);
        assertSame("Should return the same KeyPair instance", first, second);
    }
}
