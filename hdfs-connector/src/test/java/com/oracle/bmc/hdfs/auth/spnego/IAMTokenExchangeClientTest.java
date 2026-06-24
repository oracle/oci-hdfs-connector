package com.oracle.bmc.hdfs.auth.spnego;

import com.oracle.bmc.hdfs.BmcConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.junit.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class IAMTokenExchangeClientTest {

    private KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        return generator.generateKeyPair();
    }

    private IAMTokenExchangeClient createClient(HttpClient httpClient, Configuration configuration) {
        return new IAMTokenExchangeClient(httpClient, configuration);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExchangeTokenMissingClientId() throws Exception {
        Configuration conf = new Configuration();
        conf.set(BmcConstants.IAM_DOMAIN_APP_CLIENT_SECRET_KEY, "secret");
        IAMTokenExchangeClient client = createClient(mock(HttpClient.class), conf);
        client.exchangeToken("spnego", generateKeyPair());
    }

    @Test
    public void testExchangeTokenSuccess() throws Exception {
        Configuration conf = new Configuration();
        conf.set(BmcConstants.IAM_DOMAIN_APP_CLIENT_ID_KEY, "id");
        conf.set(BmcConstants.IAM_DOMAIN_APP_CLIENT_SECRET_KEY, "secret");
        conf.set(BmcConstants.IAM_TOKEN_EXCHANGE_ENDPOINT_URL_KEY, "http://host");
        conf.set(BmcConstants.TOKEN_EXCHANGE_SERVICE_ISSUER_KEY, "issuer");

        HttpClient httpClient = mock(HttpClient.class);
        HttpResponse response = new BasicHttpResponse(new BasicStatusLine(new ProtocolVersion("HTTP",1,1),200,"OK"));
        response.setEntity(new StringEntity("{\"token\":\"upst\"}"));
        when(httpClient.execute(any(HttpPost.class))).thenReturn(response);

        IAMTokenExchangeClient client = createClient(httpClient, conf);
        String token = client.exchangeToken("spnego", generateKeyPair());
        assertEquals("upst", token);
    }

    @Test(expected = InvalidClientException.class)
    public void testExchangeTokenInvalidClient() throws Exception {
        Configuration conf = new Configuration();
        conf.set(BmcConstants.IAM_DOMAIN_APP_CLIENT_ID_KEY, "id");
        conf.set(BmcConstants.IAM_DOMAIN_APP_CLIENT_SECRET_KEY, "secret");
        conf.set(BmcConstants.IAM_TOKEN_EXCHANGE_ENDPOINT_URL_KEY, "http://host");
        conf.set(BmcConstants.TOKEN_EXCHANGE_SERVICE_ISSUER_KEY, "issuer");

        HttpClient httpClient = mock(HttpClient.class);
        HttpResponse response = new BasicHttpResponse(new BasicStatusLine(new ProtocolVersion("HTTP",1,1),200,"OK"));
        response.setEntity(new StringEntity("{\"error\":\"invalid_client\",\"error_description\":\"bad\"}"));
        when(httpClient.execute(any(HttpPost.class))).thenReturn(response);

        IAMTokenExchangeClient client = createClient(httpClient, conf);
        client.exchangeToken("spnego", generateKeyPair());
    }

    @Test(expected = InvalidSpnegoTokenException.class)
    public void testExchangeTokenInvalidSpnego() throws Exception {
        Configuration conf = new Configuration();
        conf.set(BmcConstants.IAM_DOMAIN_APP_CLIENT_ID_KEY, "id");
        conf.set(BmcConstants.IAM_DOMAIN_APP_CLIENT_SECRET_KEY, "secret");
        conf.set(BmcConstants.IAM_TOKEN_EXCHANGE_ENDPOINT_URL_KEY, "http://host");
        conf.set(BmcConstants.TOKEN_EXCHANGE_SERVICE_ISSUER_KEY, "issuer");

        HttpClient httpClient = mock(HttpClient.class);
        HttpResponse response = new BasicHttpResponse(new BasicStatusLine(new ProtocolVersion("HTTP",1,1),200,"OK"));
        response.setEntity(new StringEntity("{\"error\":\"invalid_grant\"}"));
        when(httpClient.execute(any(HttpPost.class))).thenReturn(response);

        IAMTokenExchangeClient client = createClient(httpClient, conf);
        client.exchangeToken("spnego", generateKeyPair());
    }

    @Test(expected = RuntimeException.class)
    public void testExchangeTokenExecuteFailure() throws Exception {
        Configuration conf = new Configuration();
        conf.set(BmcConstants.IAM_DOMAIN_APP_CLIENT_ID_KEY, "id");
        conf.set(BmcConstants.IAM_DOMAIN_APP_CLIENT_SECRET_KEY, "secret");
        conf.set(BmcConstants.IAM_TOKEN_EXCHANGE_ENDPOINT_URL_KEY, "http://host");
        conf.set(BmcConstants.TOKEN_EXCHANGE_SERVICE_ISSUER_KEY, "issuer");

        HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.execute(any(HttpPost.class))).thenThrow(new RuntimeException("fail"));

        IAMTokenExchangeClient client = createClient(httpClient, conf);
        client.exchangeToken("spnego", generateKeyPair());
    }
}
