package com.oracle.bmc.hdfs.auth.spnego;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class UPSTManagerTest {

    private TokenGenerator tokenGenerator;
    private IAMTokenExchangeClient tokenExchangeClient;
    private RSAKeyPairGenerator rsaKeyPairGenerator;
    private UPSTManager manager;

    @Before
    public void setUp() throws Exception {
        tokenGenerator = mock(TokenGenerator.class);
        tokenExchangeClient = mock(IAMTokenExchangeClient.class);
        rsaKeyPairGenerator = mock(RSAKeyPairGenerator.class);

        UPSTManager.Builder builder = new UPSTManager.Builder()
                .tokenGenerator(tokenGenerator)
                .rsaKeyPairGenerator(rsaKeyPairGenerator)
                .tokenExchangeClient(tokenExchangeClient);

        manager = builder.build();

        KeyPair keyPair = generateRealKeyPair();
        when(rsaKeyPairGenerator.generateKeyPair()).thenReturn(keyPair);
        when(tokenExchangeClient.exchangeToken(any(), eq(keyPair))).thenReturn("upstToken");
    }

    @Test
    public void testGenerateSpnegoToken() throws IOException, InterruptedException {
        String expectedToken = "spnegoToken";
        when(tokenGenerator.generateToken()).thenReturn(expectedToken);
        String result = manager.generateSpnegoToken();

        assertEquals(expectedToken, result);
    }

    @Test(expected = UnsupportedEncodingException.class)
    public void testGetUPSTTokenExchangeError() throws UnsupportedEncodingException, NoSuchAlgorithmException {
        when(tokenExchangeClient.exchangeToken(any(), any(KeyPair.class))).thenThrow(new UnsupportedEncodingException("Token exchange failed"));
        manager.getUPSTToken("spnegoToken");
    }

    @Test(expected = RuntimeException.class)
    public void testGenerateRSAKeyPairError() throws NoSuchAlgorithmException, UnsupportedEncodingException {
        when(rsaKeyPairGenerator.generateKeyPair()).thenThrow(new NoSuchAlgorithmException("RSA algorithm not found"));
        manager.getUPSTToken("spnegoToken");
    }

    @Test(expected = NullPointerException.class)
    public void testGenerateRSAKeyPairReturnsNull() throws NoSuchAlgorithmException, UnsupportedEncodingException {
        when(rsaKeyPairGenerator.generateKeyPair()).thenReturn(null);
        manager.getUPSTToken("spnegoToken");
    }

    @Test
    public void testDecodeUPSTAndGetSessionExp() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss z");
        String currentDate = sdf.format(new Date());

        JSONObject jsonPayload = new JSONObject();
        jsonPayload.put("sess_exp", currentDate);

        String payload = Base64.getEncoder().encodeToString(jsonPayload.toString().getBytes());
        String fakeToken = "header." + payload + ".signature"; // a fake token but same format as UPST
        String result = manager.decodeUPSTAndGetSessionExp(fakeToken);

        assertEquals(currentDate, result);
    }

    @Test(expected = RuntimeException.class)
    public void testDecodeUPSTAndGetSessionExpWithInvalidToken() {
        String invalidToken = "invalid_token";
        manager.decodeUPSTAndGetSessionExp(invalidToken);
    }

    private KeyPair generateRealKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        return keyPairGenerator.generateKeyPair();
    }
}
