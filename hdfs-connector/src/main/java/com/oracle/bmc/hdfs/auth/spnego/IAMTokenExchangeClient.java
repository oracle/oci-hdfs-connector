package com.oracle.bmc.hdfs.auth.spnego;

import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.hdfs.store.BmcPropertyAccessor;
import io.netty.channel.ConnectTimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.HttpRequestRetryHandler;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.HttpResponse;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;

import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Objects;
import java.net.URLEncoder;

/**
 * A client to facilitate the exchange of SPNEGO tokens for UPST tokens by interacting with the IAM Token Exchange service.
 */
public class IAMTokenExchangeClient implements TokenExchangeService {
    private static final Logger logger = LoggerFactory.getLogger(IAMTokenExchangeClient.class);
    private final HttpClient httpClient;
    private final BmcPropertyAccessor propertyAccessor;

    public IAMTokenExchangeClient(HttpClient httpClient, Configuration configuration) {
        this.httpClient = httpClient;
        this.propertyAccessor = new BmcPropertyAccessor(configuration, null);
    }

    public IAMTokenExchangeClient(Configuration configuration) {
        SimpleRetryConfiguration simpleRetryConfig = new SimpleRetryConfiguration(3,
                new HashSet<>(Arrays.asList(
                        InterruptedIOException.class,
                        UnknownHostException.class,
                        ConnectTimeoutException.class,
                        SocketException.class
                )));

        HttpRequestRetryHandler customRetryHandler = (exception, executionCount, context) -> simpleRetryConfig.shouldRetryOnException(exception, executionCount);

        this.httpClient = HttpClients.custom()
                .setRetryHandler(customRetryHandler)
                .build();
        this.propertyAccessor = new BmcPropertyAccessor(configuration, null);
    }

    @Override
    public String exchangeToken(String spnegoToken, KeyPair keyPair) throws UnsupportedEncodingException {
        logger.info("Initiating token exchange.");
        String clientId = propertyAccessor.asString().get(BmcProperties.IAM_DOMAIN_APP_CLIENT_ID);
        String secret = propertyAccessor.asString().get(BmcProperties.IAM_DOMAIN_APP_CLIENT_SECRET);
        String domain_url = propertyAccessor.asString().get(BmcProperties.IAM_TOKEN_EXCHANGE_ENDPOINT_URL);
        String servicePrincipal = propertyAccessor.asString().get(BmcProperties.TOKEN_EXCHANGE_SERVICE_PRINCIPAL);

        if (clientId == null || secret == null) {
            logger.error("Invalid token exchange configuration.");
            throw new IllegalArgumentException("Client ID or Client Secret not provided.");
        }

        return exchangeTokenWithIAMDomainApp(spnegoToken, clientId, secret, domain_url, keyPair);
    }

    public String exchangeTokenWithIAMDomainApp(String spnegoToken, String clientId, String secret, String domainURL, KeyPair keyPair) throws UnsupportedEncodingException {
        Objects.requireNonNull(spnegoToken, "SPNEGO token must not be null");
        Objects.requireNonNull(clientId, "Client ID must not be null");
        Objects.requireNonNull(secret, "Client secret must not be null");

        HttpPost post = createHttpPostForIAMDomainApp(spnegoToken, clientId, secret, domainURL, keyPair);

        try {
            HttpResponse response = httpClient.execute(post);
            return extractTokenFromResponse(EntityUtils.toString(response.getEntity()));
        } catch (org.apache.http.client.ClientProtocolException e) {
            logger.error("Issue with the provided domain URL or other configurations: " + domainURL, e);
            throw new IllegalArgumentException("There was an issue with the provided domain URL or other configurations. Please check and try again.", e);
        } catch (InvalidClientException | InvalidSpnegoTokenException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Error exchanging token", e);
            throw new RuntimeException("Error during token exchange. Please verify your configurations and details provided and try again.", e);
        }
    }

    private HttpPost createHttpPostForIAMDomainApp(String spnegoToken, String clientId, String secret, String domainURL, KeyPair keyPair) throws UnsupportedEncodingException {
        String basicAuth = "Basic " + Base64.getEncoder().encodeToString((clientId + ":" + secret).getBytes());
        String requestBody = getRequestBody(spnegoToken, keyPair);

        HttpPost post = new HttpPost(domainURL);
        post.addHeader("Content-Type", "application/x-www-form-urlencoded;charset=utf-8");
        post.addHeader("Authorization", basicAuth);
        StringEntity entity = new StringEntity(requestBody);
        entity.setContentType(new BasicHeader(HTTP.CONTENT_TYPE, "application/x-www-form-urlencoded"));
        post.setEntity(entity);

        return post;
    }

    private String extractTokenFromResponse(String response) {
        try {
            JSONObject jsonResponse = new JSONObject(response);

            if (jsonResponse.has("error")) {
                String error = jsonResponse.getString("error");
                String errorDescription = jsonResponse.optString("error_description", "No description provided");

                logger.error("Error during token exchange: {} - {}", error, errorDescription);

                if ("invalid_client".equals(error)) {
                    throw new InvalidClientException("Client authentication failed: " + errorDescription);
                } else {
                    throw new InvalidSpnegoTokenException("Error during token exchange: " + errorDescription);
                }
            }
            return jsonResponse.getString("token");
        } catch (JSONException e) {
            logger.error("Error extracting token from response", e);
            throw new InvalidSpnegoTokenException("Failed to extract token from response. Please verify your configurations and try again.", e);
        }
    }

    private String getRequestBody(String spnegoToken, KeyPair keyPair) throws UnsupportedEncodingException {
        String publicKey = Base64.getEncoder().encodeToString(keyPair.getPublic().getEncoded());

        StringBuilder requestBody = new StringBuilder()
                .append("grant_type=").append(URLEncoder.encode("urn:ietf:params:oauth:grant-type:token-exchange", StandardCharsets.UTF_8.name()))
                .append("&requested_token_type=").append(URLEncoder.encode("urn:oci:token-type:oci-upst", StandardCharsets.UTF_8.name()))
                .append("&public_key=").append(URLEncoder.encode(publicKey, StandardCharsets.UTF_8.name()))
                .append("&subject_token=").append(URLEncoder.encode(spnegoToken, StandardCharsets.UTF_8.name()))
                .append("&subject_token_type=").append(URLEncoder.encode("spnego", StandardCharsets.UTF_8.name()))
                .append("&issuer=").append(URLEncoder.encode(propertyAccessor.asString().get(BmcProperties.TOKEN_EXCHANGE_SERVICE_ISSUER), StandardCharsets.UTF_8.name()));

        return requestBody.toString();
    }
}
