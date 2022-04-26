/**
 * Copyright (c) 2016, 2022, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.oracle.bmc.ClientConfiguration;
import com.oracle.bmc.ClientRuntime;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider;
import com.oracle.bmc.auth.SimplePrivateKeySupplier;
import com.oracle.bmc.hdfs.BmcConstants;
import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.hdfs.waiter.ResettingExponentialBackoffStrategy;
import com.oracle.bmc.http.*;
import com.oracle.bmc.http.internal.ResponseHelper;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.retrier.RetryConfiguration;
import com.oracle.bmc.util.StreamUtils;
import com.oracle.bmc.waiter.ExponentialBackoffDelayStrategy;
import com.oracle.bmc.waiter.MaxTimeTerminationStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.glassfish.jersey.apache.connector.ApacheConnectionClosingStrategy;
import org.glassfish.jersey.logging.LoggingFeature;

import javax.ws.rs.core.MediaType;

import static com.oracle.bmc.Region.enableInstanceMetadataService;
import static com.oracle.bmc.auth.AbstractFederationClientAuthenticationDetailsProviderBuilder.AUTHORIZATION_HEADER_VALUE;
import static com.oracle.bmc.auth.AbstractFederationClientAuthenticationDetailsProviderBuilder.METADATA_SERVICE_BASE_URL;
import static com.oracle.bmc.auth.AbstractFederationClientAuthenticationDetailsProviderBuilder.simpleRetry;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;

/**
 * Factory class to create a {@link BmcDataStore}. This factory allows for the usage of custom classes to
 * communicate/authenticate with Object Store, else falling back to defaults.
 */
@Slf4j
@RequiredArgsConstructor
public class BmcDataStoreFactory {
    private static final String OCI_PROPERTIES_FILE_NAME = "oci.properties";
    private final Configuration configuration;

    public static final com.oracle.bmc.Service SERVICE =
            com.oracle.bmc.Services.serviceBuilder()
                    .serviceName("OBJECTSTORAGE")
                    .serviceEndpointPrefix("objectstorage")
                    .serviceEndpointTemplate("https://objectstorage.{region}.{secondLevelDomain}")
                    .build();

    /**
     * Creates a new {@link BmcDataStore} for the given namespace and bucket.
     *
     * @param namespace
     *            The namespace.
     * @param bucket
     *            The bucket.
     * @param statistics
     *            The statistics instance to write metrics to
     * @return A new data store client.
     */
    public BmcDataStore createDataStore(
            final String namespace, final String bucket, final Statistics statistics) {
        this.setConnectorVersion();
        // override matches the same order as the filesystem name, ie, "oci://bucket@namespace"
        // so overriding property foobar is done by specifying foobar.bucket.namespace
        final String propertyOverrideSuffix = "." + bucket + "." + namespace;
        final BmcPropertyAccessor propertyAccessor =
                new BmcPropertyAccessor(this.configuration, propertyOverrideSuffix);
        return new BmcDataStore(
                propertyAccessor,
                this.createClient(propertyAccessor),
                namespace,
                bucket,
                statistics);
    }

    @VisibleForTesting
    ObjectStorage createClient(final BmcPropertyAccessor propertyAccessor) {
        final String customObjectStoreClient =
                propertyAccessor.asString().get(BmcProperties.OBJECT_STORE_CLIENT_CLASS);
        final ObjectStorage objectStorage =
                (customObjectStoreClient != null)
                        ? (ObjectStorage) this.createClass(customObjectStoreClient)
                        : buildClient(propertyAccessor);

        final String endpoint = getEndpoint(propertyAccessor.asString());
        LOG.info("Using endpoint {}", endpoint);
        objectStorage.setEndpoint(endpoint);

        return objectStorage;
    }

    private String getEndpoint(final BmcPropertyAccessor.Accessor<String> propertyAccessor) {
        if (propertyAccessor.get(BmcProperties.HOST_NAME) != null) {
            LOG.info("Getting endpoint using {}", BmcConstants.HOST_NAME_KEY);
            return propertyAccessor.get(BmcProperties.HOST_NAME);
        }

        if (propertyAccessor.get(BmcProperties.REGION_CODE_OR_ID) != null) {
            LOG.info("Getting endpoint using {}", BmcConstants.REGION_CODE_OR_ID_KEY);
            Region region =
                    Region.fromRegionCodeOrId(
                            propertyAccessor.get(BmcProperties.REGION_CODE_OR_ID));
            com.google.common.base.Optional<String> endpoint = region.getEndpoint(SERVICE);
            if (endpoint.isPresent()) {
                return endpoint.get();
            } else {
                throw new IllegalArgumentException(
                        "Endpoint for " + SERVICE + " is not known in region " + region);
            }
        }

        enableInstanceMetadataService();
        LOG.info(
                "Requesting region code from IMDS at {}",
                METADATA_SERVICE_BASE_URL + "instance/region");
        String regionCode =
                simpleRetry(
                        base -> {
                            String regionCodeUsingImds =
                                    base.path("region")
                                            .request(MediaType.APPLICATION_JSON)
                                            .header(AUTHORIZATION, AUTHORIZATION_HEADER_VALUE)
                                            .get(String.class);
                            return regionCodeUsingImds;
                        },
                        METADATA_SERVICE_BASE_URL,
                        "region");
        String endpoint = Region.formatDefaultRegionEndpoint(SERVICE, regionCode);
        LOG.info("Endpoint using Instance Metadata Service is {}", endpoint);
        return endpoint;
    }

    private ObjectStorage buildClient(final BmcPropertyAccessor propertyAccessor) {
        final ClientConfiguration.ClientConfigurationBuilder clientConfigurationBuilder =
                ClientConfiguration.builder();

        final Integer connectionTimeoutMillis =
                propertyAccessor.asInteger().get(BmcProperties.CONNECTION_TIMEOUT_MILLIS);
        if (connectionTimeoutMillis != null) {
            LOG.info("Setting connection timeout to {}", connectionTimeoutMillis);
            clientConfigurationBuilder.connectionTimeoutMillis(connectionTimeoutMillis);
        }

        final Integer readTimeoutMillis =
                propertyAccessor.asInteger().get(BmcProperties.READ_TIMEOUT_MILLIS);
        if (readTimeoutMillis != null) {
            LOG.info("Setting read timeout to {}", readTimeoutMillis);
            clientConfigurationBuilder.readTimeoutMillis(readTimeoutMillis);
        }

        // Set the retry strategy for the client
        final long retryTimeoutInSeconds =
                propertyAccessor.asLong().get(BmcProperties.RETRY_TIMEOUT_IN_SECONDS);

        // If the retryMaxSleepInSeconds is not defined, then go for ExponentialBackOffDelayStrategy, otherwise
        // go for ResettingExponential (Delay exponentially goes up till retryMaxSleepInSeconds and then resets)
        final long resetThresholdInSeconds =
                propertyAccessor
                        .asLong()
                        .get(BmcProperties.RETRY_TIMEOUT_RESET_THRESHOLD_IN_SECONDS);

        LOG.info("Setting retry timeout to {} seconds", retryTimeoutInSeconds);
        clientConfigurationBuilder.retryConfiguration(
                RetryConfiguration.builder()
                        .terminationStrategy(
                                MaxTimeTerminationStrategy.ofSeconds(retryTimeoutInSeconds))
                        .delayStrategy(
                                resetThresholdInSeconds <= 0
                                        ? new ExponentialBackoffDelayStrategy(
                                                Duration.ofSeconds(retryTimeoutInSeconds)
                                                        .toMillis())
                                        : new ResettingExponentialBackoffStrategy(
                                                resetThresholdInSeconds))
                        .build());

        final ClientConfiguration clientConfig = clientConfigurationBuilder.build();
        final BasicAuthenticationDetailsProvider authDetailsProvider =
                this.createAuthenticator(propertyAccessor);

        final String httpProxyUri = propertyAccessor.asString().get(BmcProperties.HTTP_PROXY_URI);
        ObjectStorageClient.Builder objectStorageBuilder =
                ObjectStorageClient.builder().configuration(clientConfig);

        if (propertyAccessor.asBoolean().get(BmcProperties.JERSEY_CLIENT_LOGGING_ENABLED)) {
            objectStorageBuilder.additionalClientConfigurator(
                    new JerseyLoggingClientConfigurator(
                            LoggingFeature.Verbosity.valueOf(
                                    propertyAccessor
                                            .asString()
                                            .get(BmcProperties.JERSEY_CLIENT_LOGGING_VERBOSITY)),
                            propertyAccessor
                                    .asString()
                                    .get(BmcProperties.JERSEY_CLIENT_LOGGING_LEVEL)));
        }

        final Integer apacheMaxConnectionPoolSize =
                propertyAccessor.asInteger().get(BmcProperties.APACHE_MAX_CONNECTION_POOL_SIZE);
        // If the Jersery client default connector property is enabled, change the clientConfigurator to
        // JerseyDefaultClientConfigurator. This is to support the jersey default HttpUrlConnector
        if (propertyAccessor
                .asBoolean()
                .get(BmcProperties.JERSEY_CLIENT_DEFAULT_CONNECTOR_ENABLED)) {
            objectStorageBuilder.clientConfigurator(
                    new JerseyDefaultConnectorConfigurator.NonBuffering());
        }
        // Note : Connection pooling is only supported for Apache Clients (hence, this is in else if)
        // If Jersey default connector is enabled, disregard this property
        else if (apacheMaxConnectionPoolSize != null && apacheMaxConnectionPoolSize > 0) {
            final String apacheConnectionClosingStrategyString =
                    propertyAccessor
                            .asString()
                            .get(BmcProperties.APACHE_CONNECTION_CLOSING_STRATEGY);
            ApacheConnectionClosingStrategy apacheConnectionClosingStrategy = null;
            if (apacheConnectionClosingStrategyString.equalsIgnoreCase("GRACEFUL")) {
                apacheConnectionClosingStrategy =
                        new ApacheConnectionClosingStrategy.GracefulClosingStrategy();
            } else {
                apacheConnectionClosingStrategy =
                        new ApacheConnectionClosingStrategy.ImmediateClosingStrategy();
            }
            ApacheConnectionPoolConfig apacheConnectionPoolConfig =
                    ApacheConnectionPoolConfig.newDefault()
                            .builder()
                            .totalOpenConnections(apacheMaxConnectionPoolSize)
                            .defaultMaxConnectionsPerRoute(apacheMaxConnectionPoolSize)
                            .build();
            objectStorageBuilder.clientConfigurator(
                    new ApacheConfigurator.NonBuffering(
                            ApacheConnectorProperties.builder()
                                    .connectionClosingStrategy(apacheConnectionClosingStrategy)
                                    .connectionPoolConfig(apacheConnectionPoolConfig)
                                    .build()));
        }

        if (!propertyAccessor.asBoolean().get(BmcProperties.OBJECT_AUTO_CLOSE_INPUT_STREAM)) {
            ResponseHelper.shouldAutoCloseResponseInputStream(false);
        }
        // If a proxy is not defined, use the existing ObjectStorageClient that leverages the DefaultConnectorProvider.
        // Else, build an ObjectStorageClient that leverages the ApacheConnector to configure a proxy.
        return (StringUtils.isBlank(httpProxyUri))
                ? objectStorageBuilder.build(authDetailsProvider)
                : buildClientWithProxy(
                        authDetailsProvider, clientConfig, propertyAccessor, httpProxyUri);
    }

    private ObjectStorage buildClientWithProxy(
            final BasicAuthenticationDetailsProvider authDetailsProvider,
            final ClientConfiguration clientConfig,
            final BmcPropertyAccessor propertyAccessor,
            final String httpProxyUri) {
        LOG.info("Setting HTTP Proxy URI to {}", httpProxyUri);
        final ClientConfigDecorator poolConfigDecorator =
                newApacheConnectionPoolingClientConfigDecorator(clientConfig);
        final ClientConfigDecorator proxyConfigDecorator =
                newApacheProxyConfigDecorator(httpProxyUri, propertyAccessor);
        final List<ClientConfigDecorator> clientConfigDecorators =
                Lists.newArrayList(poolConfigDecorator, proxyConfigDecorator);

        final ClientConfigurator clientConfigurator =
                new ApacheConfigurator(clientConfigDecorators);

        ObjectStorageClient.Builder objectStorageBuilder =
                ObjectStorageClient.builder()
                        .configuration(clientConfig)
                        .clientConfigurator(clientConfigurator);

        if (propertyAccessor.asBoolean().get(BmcProperties.JERSEY_CLIENT_LOGGING_ENABLED)) {
            objectStorageBuilder.additionalClientConfigurator(
                    new JerseyLoggingClientConfigurator(
                            LoggingFeature.Verbosity.valueOf(
                                    propertyAccessor
                                            .asString()
                                            .get(BmcProperties.JERSEY_CLIENT_LOGGING_VERBOSITY)),
                            propertyAccessor
                                    .asString()
                                    .get(BmcProperties.JERSEY_CLIENT_LOGGING_LEVEL)));
        }

        return objectStorageBuilder.build(authDetailsProvider);
    }

    // A pool config is required as the default pool config for Hadoop versions > 2.8.x keeps connections alive
    // indefinitely causing the job to remain stuck in a RUNNING state.
    private ClientConfigDecorator newApacheConnectionPoolingClientConfigDecorator(
            final ClientConfiguration clientConfig) {
        final int maxConns = clientConfig.getMaxAsyncThreads();
        final ApacheConnectionPoolConfig poolConfig =
                ApacheConnectionPoolConfig.builder()
                        // Setting maxConnsPerRoute to match the total open connections as the HDFS connector will
                        // only connect to one endpoint.
                        .defaultMaxConnectionsPerRoute(maxConns)
                        .totalOpenConnections(maxConns)
                        .ttlInMillis(clientConfig.getConnectionTimeoutMillis())
                        .build();
        return new ApacheConnectionPoolingClientConfigDecorator(poolConfig);
    }

    private ClientConfigDecorator newApacheProxyConfigDecorator(
            final String httpProxyUri, final BmcPropertyAccessor propertyAccessor) {
        final ApacheProxyConfig proxyConfig =
                ApacheProxyConfig.builder()
                        .uri(httpProxyUri)
                        .username(
                                propertyAccessor.asString().get(BmcProperties.HTTP_PROXY_USERNAME))
                        .password(
                                propertyAccessor.asString().get(BmcProperties.HTTP_PROXY_PASSWORD))
                        .build();
        return new ApacheProxyConfigDecorator(proxyConfig);
    }

    // set the connector version onto the SDK.
    private void setConnectorVersion() {
        InputStream propertyStream = null;
        try {
            propertyStream =
                    getClass()
                            .getClassLoader()
                            .getResourceAsStream("com/oracle/bmc/hdfs/" + OCI_PROPERTIES_FILE_NAME);

            final Properties properties = new Properties();
            String connectorVersion;
            try {
                properties.load(propertyStream);
                connectorVersion = properties.getProperty("connector.version", "0.0.0");
            } catch (Exception e) {
                LOG.error("Failed to load " + OCI_PROPERTIES_FILE_NAME, e);
                connectorVersion = "0.0.0";
            }
            LOG.info("Using connector version: {}", connectorVersion);
            ClientRuntime.setClientUserAgent(
                    String.format("Oracle-HDFS_Connector/%s", connectorVersion));
        } finally {
            StreamUtils.closeQuietly(propertyStream);
        }
    }

    @VisibleForTesting
    BasicAuthenticationDetailsProvider createAuthenticator(BmcPropertyAccessor propertyAccessor) {
        final String customAuthenticator =
                propertyAccessor.asString().get(BmcProperties.OBJECT_STORE_AUTHENTICATOR_CLASS);
        if (customAuthenticator != null) {
            return this.createClass(customAuthenticator);
        }

        final String tenantId = propertyAccessor.asString().get(BmcProperties.TENANT_ID);
        final String userId = propertyAccessor.asString().get(BmcProperties.USER_ID);
        final String fingerprint = propertyAccessor.asString().get(BmcProperties.FINGERPRINT);
        if (tenantId == null || userId == null || fingerprint == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Must specify tenantId (%s), userId (%s), and fingerprint (%s) in configuration",
                            tenantId,
                            userId,
                            fingerprint));
        }

        final String pemFilePath = propertyAccessor.asString().get(BmcProperties.PEM_FILE_PATH);
        if (pemFilePath == null) {
            throw new IllegalArgumentException("Must specify PEM file path");
        }

        final Supplier<InputStream> supplier = new SimplePrivateKeySupplier(pemFilePath);
        final char[] passPhrase = propertyAccessor.asPassword().get(BmcProperties.PASS_PHRASE);

        return SimpleAuthenticationDetailsProvider.builder()
                .tenantId(tenantId)
                .userId(userId)
                .fingerprint(fingerprint)
                .privateKeySupplier(supplier)
                .passphraseCharacters(passPhrase)
                .build();
    }

    @SuppressWarnings("unchecked")
    private <T> T createClass(final String className) {
        try {
            final Class<?> customClass = Class.forName(className);
            final Constructor<?> customClassConstructor =
                    customClass.getConstructor(Configuration.class);
            try {
                return (T) customClassConstructor.newInstance(this.configuration);
            } catch (InstantiationException
                    | IllegalAccessException
                    | IllegalArgumentException
                    | InvocationTargetException e) {
                throw new IllegalStateException("Unable to create new custom client instance", e);
            }
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Configured to create custom class '" + className + "', but none exists");
        } catch (final NoSuchMethodException e) {
            throw new IllegalStateException(
                    "Custom client class does not have the required constructor", e);
        } catch (final SecurityException e) {
            throw new IllegalStateException("Unable to create new custom client instance", e);
        }
    }
}
