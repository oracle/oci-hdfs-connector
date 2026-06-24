package com.oracle.bmc.hdfs.auth.spnego;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class UPSTAuthenticationDetailsProviderTest {

    private UPSTAuthenticationDetailsProvider provider;
    private Configuration configuration;
    private UPSTManagerFactory upstManagerFactory;
    private UPSTManager upstManager;

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        upstManagerFactory = mock(UPSTManagerFactory.class);
        upstManager = mock(UPSTManager.class);

        when(upstManagerFactory.createUPSTManager()).thenReturn(upstManager);

        provider = new UPSTAuthenticationDetailsProvider(configuration, upstManagerFactory);
    }

    @Test
    public void testRefresh() throws Exception {
        String expectedSpnegoToken = "mocked_spnego_token";
        String expectedUPSTToken = "mocked_upst_token";
        UPSTResponse mockedResponse = new UPSTResponse(expectedUPSTToken, null, "sample_exp");

        when(upstManager.generateSpnegoToken()).thenReturn(expectedSpnegoToken);
        when(upstManager.getUPSTToken(expectedSpnegoToken)).thenReturn(mockedResponse);

        String actualUPSTToken = provider.refresh();

        assertEquals(expectedUPSTToken, actualUPSTToken);
    }

    @Test
    public void testRefreshWhenSpnegoTokenGenerationFails() throws Exception {
        when(upstManager.generateSpnegoToken()).thenThrow(new RuntimeException("Failed to generate SPNEGO token"));
        String result = provider.refresh();
        assertNull(result);
    }

    @Test
    public void testGettersAfterSuccessfulRefresh() throws Exception {
        String spnegoToken = "spnego";
        KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();
        String upstToken = "upstToken";
        String sessionExp = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss z")
                .format(new Date(System.currentTimeMillis() + 60000));
        UPSTResponse response = new UPSTResponse(upstToken, privateKey, sessionExp);

        when(upstManager.generateSpnegoToken()).thenReturn(spnegoToken);
        when(upstManager.getUPSTToken(spnegoToken)).thenReturn(response);

        provider.refresh();

        assertEquals(upstToken, provider.getKeyId());

        InputStream is = provider.getPrivateKey();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int b;
        while ((b = is.read()) != -1) {
            baos.write(b);
        }
        assertArrayEquals(UPSTResponse.toByteArrayFromRSAPrivateKey(privateKey), baos.toByteArray());

        assertEquals(sessionExp, provider.getSessionExp());

        long expected = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss z")
                .parse(sessionExp).getTime() - System.currentTimeMillis() - (5 * 60 * 1000);
        long actual = provider.getTimeUntilRefresh();
        assertTrue(Math.abs(expected - actual) < 1000);
    }

}
