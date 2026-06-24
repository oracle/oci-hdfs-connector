package com.oracle.bmc.hdfs.auth.spnego;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class UPSTAuthenticationCustomAuthenticatorTest {
    private UPSTAuthenticationCustomAuthenticator authenticator;
    private UPSTAuthenticationDetailsProvider mockProvider;

    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        authenticator = new UPSTAuthenticationCustomAuthenticator(conf);

        mockProvider = mock(UPSTAuthenticationDetailsProvider.class);
        Field field = UPSTAuthenticationCustomAuthenticator.class.getDeclaredField("upstAuthProvider");
        field.setAccessible(true);
        field.set(authenticator, mockProvider);
    }

    @Test
    public void testGetKeyIdPrefixesST() {
        when(mockProvider.getKeyId()).thenReturn("token");
        String keyId = authenticator.getKeyId();
        assertEquals("ST$token", keyId);
    }

    @Test
    public void testGetPrivateKeyDelegates() {
        InputStream stream = new ByteArrayInputStream(new byte[] {1,2,3});
        when(mockProvider.getPrivateKey()).thenReturn(stream);
        assertSame(stream, authenticator.getPrivateKey());
    }

    @Test
    public void testGetPassPhraseReturnsNull() {
        assertNull(authenticator.getPassPhrase());
    }

    @Test
    public void testGetPassphraseCharactersDelegates() {
        char[] chars = new char[] {'a','b','c'};
        when(mockProvider.getPassphraseCharacters()).thenReturn(chars);
        assertArrayEquals(chars, authenticator.getPassphraseCharacters());
    }

    @Test
    public void testRefreshDelegates() {
        when(mockProvider.refresh()).thenReturn("refreshed");
        String token = authenticator.refresh();
        assertEquals("refreshed", token);
    }
}