/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.oracle.bmc.ClientConfiguration;
import com.oracle.bmc.ClientRuntime;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.SessionTokenAuthenticationDetailsProvider;
import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider;
import com.oracle.bmc.auth.SimplePrivateKeySupplier;
import com.oracle.bmc.auth.internal.DelegationTokenConfigurator;
import com.oracle.bmc.hdfs.BmcConstants;
import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.hdfs.monitoring.OCIMonitorConsumerPlugin;
import com.oracle.bmc.hdfs.monitoring.OCIMonitorPlugin;
import com.oracle.bmc.hdfs.monitoring.OCIMonitorPluginHandler;
import com.oracle.bmc.hdfs.waiter.ResettingExponentialBackoffStrategy;
import com.oracle.bmc.http.ClientConfigurator;
import com.oracle.bmc.http.client.Options;
import com.oracle.bmc.http.client.ProxyConfiguration;
import com.oracle.bmc.http.client.StandardClientProperties;
import com.oracle.bmc.http.client.jersey.ApacheClientProperties;
import com.oracle.bmc.http.client.jersey.JerseyClientProperties;
import com.oracle.bmc.http.client.jersey.JerseyClientProperty;
import com.oracle.bmc.http.client.jersey.apacheconfigurator.ApacheConnectionPoolConfig;
import com.oracle.bmc.monitoring.MonitoringClient;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.retrier.RetryConfiguration;
import com.oracle.bmc.util.CircuitBreakerUtils;
import com.oracle.bmc.util.StreamUtils;
import com.oracle.bmc.util.internal.FileUtils;
import com.oracle.bmc.util.internal.StringUtils;
import com.oracle.bmc.waiter.ExponentialBackoffDelayStrategy;
import com.oracle.bmc.waiter.ExponentialBackoffDelayStrategyWithJitter;
import com.oracle.bmc.waiter.MaxTimeTerminationStrategy;
import com.oracle.bmc.waiter.WaiterConfiguration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.glassfish.jersey.apache.connector.ApacheConnectionClosingStrategy;
import org.glassfish.jersey.logging.LoggingFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

import static com.oracle.bmc.Region.enableInstanceMetadataService;
import static com.oracle.bmc.auth.AbstractFederationClientAuthenticationDetailsProviderBuilder.AUTHORIZATION_HEADER_VALUE;
import static com.oracle.bmc.auth.AbstractFederationClientAuthenticationDetailsProviderBuilder.METADATA_SERVICE_BASE_URL;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static org.glassfish.jersey.logging.LoggingFeature.LOGGING_FEATURE_LOGGER_LEVEL_CLIENT;
import static org.glassfish.jersey.logging.LoggingFeature.LOGGING_FEATURE_VERBOSITY_CLIENT;

/**
 * Factory class to create a {@link BmcDataStore}. This factory allows for the usage of custom classes to
 * communicate/authenticate with Object Store, else falling back to defaults.
 */
@Slf4j
@RequiredArgsConstructor
public class BmcDataStoreFactory {
    private static final String OCI_PROPERTIES_FILE_NAME = "oci.properties";
    private final Configuration configuration;
    private final String OCI_DELEGATION_TOKEN_FILE = "OCI_DELEGATION_TOKEN_FILE";
    private String namespaceName;

    /**
     * Region codes or ID can be specified in the URI authority section following the namespace,
     * using the following format: oci://bucket@namespace.region/dir/file.
     * The region can either be a region ID (e.g., us-ashburn-1) or an airport code (e.g., IAD).
     * The precedence order for specifying the region is as follows: Region code/ID in the URI takes
     * the highest precedence, followed by the fs.oci.client.hostname property, and finally,
     * the fs.oci.client.regionCodeOrId property.
     */
    private String regionCodeOrId;

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
        namespaceName = namespace;
        this.setConnectorVersion();
        // override matches the same order as the filesystem name, ie, "oci://bucket@namespace"
        // so overriding property foobar is done by specifying foobar.bucket.namespace
        final String propertyOverrideSuffix = "." + bucket + "." + namespace;
        final BmcPropertyAccessor propertyAccessor =
                new BmcPropertyAccessor(this.configuration, propertyOverrideSuffix);
        if (propertyAccessor
                    .asBoolean()
                    .get(BmcProperties.MULTIREGION_ENABLED)) {
            LOG.info("Multi-region support enabled, parsing region code or id");
            Pattern pattern =
                    Pattern.compile("([^:\\/]+)\\.([a-zA-Z0-9-]{3,})");

            Matcher matcher = pattern.matcher(namespace);
            // Try parse namespace and region code/id
            if (matcher.find()) {
                namespaceName = matcher.group(1).trim();
                regionCodeOrId = matcher.group(2).trim();
                LOG.info("Multi-region support enabled, namespace {}, region {}", namespaceName, regionCodeOrId);
            }
        }

        OCIMonitorPluginHandler monPluginHandler = initMonitoringConsumerPlugins(propertyAccessor, bucket, namespace);

        return new BmcDataStore(
                propertyAccessor,
                this.createClient(propertyAccessor),
                namespaceName,
                bucket,
                statistics, monPluginHandler);
    }

    private OCIMonitorPluginHandler initMonitoringConsumerPlugins(BmcPropertyAccessor propertyAccessor,
                                                                  String bucketName, String ossNamespaceName) {
        OCIMonitorPluginHandler pluginHandler = new OCIMonitorPluginHandler();

        String plugins = propertyAccessor.asString().get(BmcProperties.OCI_MONITORING_CONSUMER_PLUGINS);
        if (!StringUtils.isEmpty(plugins)) {

            String monGroupingClusterID = propertyAccessor.asString().get(BmcProperties.OCI_MON_GROUPING_CLUSTER_ID);
            if (StringUtils.isEmpty(monGroupingClusterID)) {
                LOG.info("OCI monitoring framework is enabled, but the unique ID for identifying the HDFS cluster" +
                        " is not provided. Not initalizing monitoring framework.");
                pluginHandler.setEnabled(false);
                return pluginHandler;
            }

            int maxBacklogBeforeDrop = propertyAccessor.asInteger().get(BmcProperties.OCI_MON_MAX_BACKLOG_BEFORE_DROP);
            pluginHandler.setMaxBacklogBeforeDrop(maxBacklogBeforeDrop);

            List<OCIMonitorConsumerPlugin> pluginList = new ArrayList<>();

            String[] pluginClasses = plugins.split(",");

            for (String pluginClass : pluginClasses) {

                pluginClass = pluginClass.trim();

                try {
                    // Special handling for oci monitoring plugin implementation because much of
                    // init code resides inside the bmcDataStoreFactory.
                    if (pluginClass.equals("com.oracle.bmc.hdfs.monitoring.OCIMonitorPlugin")) {
                        OCIMonitorPlugin plugin = initOCIMonitoringPlugin(propertyAccessor, bucketName,
                                monGroupingClusterID, ossNamespaceName);

                        if (plugin != null) {
                            pluginList.add(plugin);
                            LOG.info("Successfully initialized {} to consume OCI metrics.", pluginClass);
                        }
                    } else {
                        Constructor pluginConstructor =
                                Class.forName(pluginClass).getConstructor(BmcPropertyAccessor.class, String.class,
                                        String.class, String.class);
                        OCIMonitorConsumerPlugin plugin = (OCIMonitorConsumerPlugin)
                                pluginConstructor.newInstance(propertyAccessor, bucketName, monGroupingClusterID,
                                        ossNamespaceName);
                        pluginList.add(plugin);
                        LOG.info("Successfully initialized {} to consume OCI metrics.", pluginClass);
                    }

                } catch (Exception e) {
                    LOG.error("Unable to initialize {} due to {}", pluginClass, e);
                }
            }

            if (pluginList.size() > 0) {
                pluginHandler.setEnabled(true);
                pluginHandler.setListOfPlugins(pluginList);
                pluginHandler.setBucketName(bucketName);
                pluginHandler.init();
                LOG.info("Initialized the OCI monitoring framework successfully.");
            } else {
                LOG.info("No plugins found for OCI montioring emit, so disabling the metrics emission.");
                pluginHandler.setEnabled(false);
            }
        }
        return pluginHandler;
    }

    private OCIMonitorPlugin initOCIMonitoringPlugin(BmcPropertyAccessor propertyAccessor, String bucketName,
                                                     String monGroupingClusterID, String ossNamespaceName) {
        try {

            String telemetryIngestionEndpoint =
                    propertyAccessor.asString().get(BmcProperties.OCI_MON_TELEMETRY_INGESTION_ENDPOINT);
            if (StringUtils.isEmpty(telemetryIngestionEndpoint)) {
                LOG.info("OCI monitoring is enabled, but the telemetry ingestion URL is undefined." +
                        " Not initalizing OCI monitoring. Example endpoint URL for Ashburn: " +
                        "https://telemetry-ingestion.us-ashburn-1.oraclecloud.com");
                return null;
            }

            String monCompartmentOCID = propertyAccessor.asString().get(BmcProperties.OCI_MON_COMPARTMENT_OCID);
            if (StringUtils.isEmpty(monCompartmentOCID)) {
                LOG.info("OCI monitoring is enabled, but the compartment OCID where monitoring is enabled" +
                        " is not provided. Not initalizing OCI monitoring.");
                return null;
            }

            String monRGName = propertyAccessor.asString().get(BmcProperties.OCI_MON_RG_NAME);
            String monNamespacename = propertyAccessor.asString().get(BmcProperties.OCI_MON_NS_NAME);

            if (StringUtils.isEmpty(monNamespacename)) {
                LOG.info("OCI monitoring is enabled, but namespace name override is invalid." +
                        " Not initializing OCI monitoring");
                return null;
            }

            boolean bucketLevelStatsEnabled = propertyAccessor.asBoolean().get(BmcProperties.OCI_MON_BUCKET_LEVEL_ENABLED);

            int emitThreadIntervalSeconds =
                    propertyAccessor.asInteger().get(BmcProperties.OCI_MON_EMIT_THREAD_POLL_INTERVAL_SECONDS);

            long maxBacklogBeforeDrop = propertyAccessor.asInteger().get(BmcProperties.OCI_MON_MAX_BACKLOG_BEFORE_DROP);

            MonitoringClient mc = getMonitoringClient(propertyAccessor);

            OCIMonitorPlugin ociMonitorPlugin = new OCIMonitorPlugin(telemetryIngestionEndpoint, monCompartmentOCID, monGroupingClusterID,
                    monRGName, monNamespacename, bucketLevelStatsEnabled, emitThreadIntervalSeconds,
                    maxBacklogBeforeDrop, mc, bucketName, propertyAccessor, ossNamespaceName);

            LOG.info("Initialized OCI monitoring with telemetryIngestionEndpoint:{} monCompartmentOCID:{} " +
                            "monGroupingClusterID:{} monRGName:{} monNamespacename:{} bucketLevelStatsEnabled:{} " +
                            "emitThreadIntervalSeconds:{} maxBacklogBeforeDrop:{}",
                    telemetryIngestionEndpoint, monCompartmentOCID, monGroupingClusterID, monRGName, monNamespacename,
                    bucketLevelStatsEnabled, emitThreadIntervalSeconds, maxBacklogBeforeDrop);

            return ociMonitorPlugin;
        } catch (Exception e) {
            LOG.error("Exception encountered when initializing OCI metrics. Proceeding without OCI metrics init.", e);
            return null;
        }
    }

    private MonitoringClient getMonitoringClient(BmcPropertyAccessor propertyAccessor) {
        final ClientConfiguration.ClientConfigurationBuilder clientConfigurationBuilder =
                ClientConfiguration.builder();

        final Integer connectionTimeoutMillis =
                propertyAccessor.asInteger().get(BmcProperties.CONNECTION_TIMEOUT_MILLIS);
        if (connectionTimeoutMillis != null) {
            LOG.info("Setting Monitoring connection timeout to {}", connectionTimeoutMillis);
            clientConfigurationBuilder.connectionTimeoutMillis(connectionTimeoutMillis);
        }

        final Integer readTimeoutMillis =
                propertyAccessor.asInteger().get(BmcProperties.READ_TIMEOUT_MILLIS);
        if (readTimeoutMillis != null) {
            LOG.info("Setting Monitoring read timeout to {}", readTimeoutMillis);
            clientConfigurationBuilder.readTimeoutMillis(readTimeoutMillis);
        }

        final Boolean circuitBreakerEnabled =
            propertyAccessor.asBoolean().get(BmcProperties.OBJECT_STORAGE_CLIENT_CIRCUIT_BREAKER_ENABLED);
        if (circuitBreakerEnabled != null && !circuitBreakerEnabled) {
            LOG.info("Setting to no-op circuit breaker conf");
            clientConfigurationBuilder.circuitBreakerConfiguration(
                CircuitBreakerUtils.getNoCircuitBreakerConfiguration());
        }

        // Retry configuration
        final long retryTimeoutInSeconds =
                propertyAccessor.asLong().get(BmcProperties.RETRY_TIMEOUT_IN_SECONDS);
        final long resetThresholdInSeconds =
                propertyAccessor.asLong().get(BmcProperties.RETRY_TIMEOUT_RESET_THRESHOLD_IN_SECONDS);

        LOG.info("Setting retry timeout to {} seconds", retryTimeoutInSeconds);
        clientConfigurationBuilder.retryConfiguration(
                RetryConfiguration.builder()
                        .terminationStrategy(MaxTimeTerminationStrategy.ofSeconds(retryTimeoutInSeconds))
                        .delayStrategy(
                                resetThresholdInSeconds <= 0
                                        ? new ExponentialBackoffDelayStrategy(Duration.ofSeconds(retryTimeoutInSeconds).toMillis())
                                        : new ResettingExponentialBackoffStrategy(resetThresholdInSeconds))
                        .build()
        );

        final ClientConfiguration clientConfig = clientConfigurationBuilder.build();
        BasicAuthenticationDetailsProvider authDetailsProvider = this.createAuthenticator(propertyAccessor);

        MonitoringClient.Builder monitoringClientBuilder = MonitoringClient.builder()
                .configuration(clientConfig);

        // Delegation Token Configurator (optional)
        String delegationTokenFilePath =
                System.getenv(OCI_DELEGATION_TOKEN_FILE) != null
                        ? System.getenv(OCI_DELEGATION_TOKEN_FILE)
                        : propertyAccessor.asString().get(BmcProperties.OCI_DELEGATION_TOKEN_FILEPATH);

        LOG.info("Monitoring delegation token file path: {}", delegationTokenFilePath);
        if (delegationTokenFilePath != null) {
            StringBuilder tokenBuilder = new StringBuilder();
            try (Stream<String> stream =
                         Files.lines(
                                 Paths.get(FileUtils.expandUserHome(delegationTokenFilePath)),
                                 StandardCharsets.UTF_8)) {
                stream.forEach(tokenBuilder::append);
            } catch (IOException e) {
                LOG.warn("Exception in reading or parsing delegation token file", e);
            }
            String delegationToken = tokenBuilder.toString();
            monitoringClientBuilder.additionalClientConfigurator(new DelegationTokenConfigurator(delegationToken));
        }

        // Handle proxy
        String httpProxyUri = propertyAccessor.asString().get(BmcProperties.HTTP_PROXY_URI);
        if (StringUtils.isEmpty(httpProxyUri)) {
            return monitoringClientBuilder.build(authDetailsProvider);
        } else {
            return buildMonitoringClientWithProxy(authDetailsProvider, clientConfig, propertyAccessor, httpProxyUri,
                    monitoringClientBuilder);
        }
    }

    @VisibleForTesting
    public ObjectStorage createClient(final BmcPropertyAccessor propertyAccessor) {
        final String customObjectStoreClient =
                propertyAccessor.asString().get(BmcProperties.OBJECT_STORE_CLIENT_CLASS);
        final ObjectStorage objectStorage =
                (customObjectStoreClient != null)
                        ? (ObjectStorage) this.createClass(customObjectStoreClient)
                        : buildClient(propertyAccessor);

        final String endpoint = getEndpoint(propertyAccessor);
        LOG.info("Using endpoint {}", endpoint);
        objectStorage.setEndpoint(endpoint);

        return objectStorage;
    }

    private String getEndpoint(final BmcPropertyAccessor propertyAccessor) {
        if (regionCodeOrId == null) {
            if (propertyAccessor.asString().get(BmcProperties.HOST_NAME) != null) {
                LOG.info("Getting endpoint using {}", BmcConstants.HOST_NAME_KEY);
                return propertyAccessor.asString().get(BmcProperties.HOST_NAME);
            }
            regionCodeOrId =  propertyAccessor.asString().get(BmcProperties.REGION_CODE_OR_ID);
        }

        if (propertyAccessor
                    .asBoolean()
                    .get(BmcProperties.REALM_SPECIFIC_ENDPOINT_TEMPLATES_ENABLED)) {
            LOG.info(
                    "Getting realm-specific endpoint template as {} flag is enabled",
                    BmcConstants.REALM_SPECIFIC_ENDPOINT_TEMPLATES_ENABLED_KEY);

            if (regionCodeOrId == null)  {
                throw new IllegalArgumentException(
                        "Property `"
                                + BmcConstants.REALM_SPECIFIC_ENDPOINT_TEMPLATES_ENABLED_KEY
                                + "` was enabled without setting the property `"
                                + BmcConstants.REGION_CODE_OR_ID_KEY
                                + "` or set the region code/id in authority. Please set the region code or id using `"
                                + BmcConstants.REGION_CODE_OR_ID_KEY
                                + "` property to enable use of realm-specific endpoint template or set it region code/id in URI");
            }
            LOG.info(
                    "Region code or id set to {} using {}",
                    regionCodeOrId,
                    BmcConstants.REGION_CODE_OR_ID_KEY);
            Region region = Region.fromRegionCodeOrId(regionCodeOrId);

            Map<String, String> realmSpecificEndpointTemplateMap =
                    ObjectStorageClient.SERVICE.getServiceEndpointTemplateForRealmMap();
            if (realmSpecificEndpointTemplateMap != null
                        && !realmSpecificEndpointTemplateMap.isEmpty()) {

                String realmId = region.getRealm().getRealmId();
                String realmSpecificEndpointTemplate =
                        realmSpecificEndpointTemplateMap.get(realmId.toLowerCase(Locale.ROOT));
                if (StringUtils.isNotBlank(realmSpecificEndpointTemplate)) {
                    LOG.info(
                            "Using realm-specific endpoint template {}",
                            realmSpecificEndpointTemplate);
                    return realmSpecificEndpointTemplate
                                   .replace("{region}", region.getRegionId())
                                   .replace("{namespaceName+Dot}", namespaceName + ".");
                } else {
                    LOG.info(
                            "{} property was enabled but realm-specific endpoint template is not defined for {} realm",
                            BmcConstants.REALM_SPECIFIC_ENDPOINT_TEMPLATES_ENABLED_KEY,
                            realmId);
                }
            } else {
                LOG.info(
                        "Not using realm-specific endpoint template, because no realm-specific endpoint template map was set, or the map was empty");
            }
        }

        if (regionCodeOrId != null) {
            LOG.info("Getting endpoint using {}", regionCodeOrId);
            Region region = Region.fromRegionCodeOrId(regionCodeOrId);
            Optional<String> endpoint = region.getEndpoint(ObjectStorageClient.SERVICE);
            return endpoint.orElseThrow(() -> new IllegalArgumentException(
                    "Endpoint for "
                            + ObjectStorageClient.SERVICE
                            + " is not known in region "
                            + region));
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
        String endpoint =
                Region.formatDefaultRegionEndpoint(ObjectStorageClient.SERVICE, regionCode);
        LOG.info("Endpoint using Instance Metadata Service is {}", endpoint);
        return endpoint;
    }

    public static <T> T simpleRetry(
            Function<WebTarget, T> retryOperation,
            final String metadataServiceUrl,
            final String endpoint) {
        final Client CLIENT = ClientBuilder.newClient();

        ExponentialBackoffDelayStrategyWithJitter strategy =
                new ExponentialBackoffDelayStrategyWithJitter(TimeUnit.SECONDS.toMillis(100));
        WaiterConfiguration.WaitContext context =
                new WaiterConfiguration.WaitContext(System.currentTimeMillis());

        final int MAX_RETRIES = 8;
        RuntimeException lastException = null;
        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            try {
                WebTarget base = CLIENT.target(metadataServiceUrl + "instance/");
                return retryOperation.apply(base);
            } catch (RuntimeException e) {
                LOG.warn(
                        "Attempt {} - Rest call to get "
                                + endpoint
                                + " from metadata service failed ",
                        (retry + 1),
                        e);
                lastException = e;
                try {
                    long waitTime = strategy.nextDelay(context);
                    Thread.sleep(waitTime);
                    context.incrementAttempts();
                    LOG.info("Exiting retry {} with wait time: {} millis", (retry + 1), waitTime);
                } catch (InterruptedException interruptedException) {
                    LOG.debug(
                            "Thread interrupted while waiting to make next call to get "
                                    + endpoint
                                    + " from instance metadata service",
                            interruptedException);
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        CLIENT.close();
        throw lastException;
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

        final Boolean circuitBreakerEnabled =
            propertyAccessor.asBoolean().get(BmcProperties.OBJECT_STORAGE_CLIENT_CIRCUIT_BREAKER_ENABLED);
        if (circuitBreakerEnabled != null && !circuitBreakerEnabled) {
            LOG.info("Setting to no-op circuit breaker conf");
            clientConfigurationBuilder.circuitBreakerConfiguration(
                CircuitBreakerUtils.getNoCircuitBreakerConfiguration());
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
        final long retryJitterInMillis =
                propertyAccessor
                        .asLong()
                        .get(BmcProperties.RETRY_JITTER_IN_MILLIS);

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
                                                resetThresholdInSeconds, retryJitterInMillis))
                        .build());

        final ClientConfiguration clientConfig = clientConfigurationBuilder.build();
        final BasicAuthenticationDetailsProvider authDetailsProvider =
                this.createAuthenticator(propertyAccessor);

        final String httpProxyUri = propertyAccessor.asString().get(BmcProperties.HTTP_PROXY_URI);
        ObjectStorageClient.Builder objectStorageBuilder =
                ObjectStorageClient.builder().configuration(clientConfig);

        if (propertyAccessor.asBoolean().get(BmcProperties.JERSEY_CLIENT_LOGGING_ENABLED)) {
            objectStorageBuilder.additionalClientConfigurator(
                    builder -> {
                        builder.property(
                                JerseyClientProperty.create(LOGGING_FEATURE_VERBOSITY_CLIENT),
                                LoggingFeature.Verbosity.valueOf(
                                        propertyAccessor
                                                .asString()
                                                .get(
                                                        BmcProperties
                                                                .JERSEY_CLIENT_LOGGING_VERBOSITY)));
                        builder.property(
                                JerseyClientProperty.create(LOGGING_FEATURE_LOGGER_LEVEL_CLIENT),
                                propertyAccessor
                                        .asString()
                                        .get(BmcProperties.JERSEY_CLIENT_LOGGING_LEVEL));
                    });
        }

        final Integer apacheMaxConnectionPoolSize =
                propertyAccessor.asInteger().get(BmcProperties.APACHE_MAX_CONNECTION_POOL_SIZE);
        // If the Jersery client default connector property is enabled, change the clientConfigurator to
        // JerseyDefaultClientConfigurator. This is to support the jersey default HttpUrlConnector
        if (propertyAccessor
                .asBoolean()
                .get(BmcProperties.JERSEY_CLIENT_DEFAULT_CONNECTOR_ENABLED)) {
            objectStorageBuilder.clientConfigurator(
                    builder -> {
                        builder.property(JerseyClientProperties.USE_APACHE_CONNECTOR, false);
                        builder.property(StandardClientProperties.BUFFER_REQUEST, false);
                    });
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
            PoolingHttpClientConnectionManager poolConnectionManager =
                    new PoolingHttpClientConnectionManager();
            poolConnectionManager.setMaxTotal(apacheMaxConnectionPoolSize);
            poolConnectionManager.setDefaultMaxPerRoute(apacheMaxConnectionPoolSize);

            ApacheConnectionClosingStrategy finalApacheConnectionClosingStrategy =
                    apacheConnectionClosingStrategy;
            objectStorageBuilder.clientConfigurator(
                    builder -> {
                        builder.property(StandardClientProperties.BUFFER_REQUEST, false);
                        builder.property(
                                ApacheClientProperties.CONNECTION_MANAGER, poolConnectionManager);
                        builder.property(
                                ApacheClientProperties.CONNECTION_CLOSING_STRATEGY,
                                finalApacheConnectionClosingStrategy);
                        builder.property(ApacheClientProperties.REUSE_STRATEGY, null);
                        builder.property(ApacheClientProperties.RETRY_HANDLER, null);
                    });
        }

        // To use DelegationTokenConfigurator, check for the OCI_DELEGATION_TOKEN_FILE key in the environment variable.
        // If the it does not exist in environment variable, check if OCI_DELEGATION_TOKEN_FILEPATH is set as a property.
        String delegationTokenFilePath =
                System.getenv(OCI_DELEGATION_TOKEN_FILE) != null
                        ? System.getenv(OCI_DELEGATION_TOKEN_FILE)
                        : propertyAccessor
                                .asString()
                                .get(BmcProperties.OCI_DELEGATION_TOKEN_FILEPATH);
        LOG.info("Delegation token file path: {}", delegationTokenFilePath);
        if (delegationTokenFilePath != null) {
            StringBuilder tokenBuilder = new StringBuilder();
            try (Stream<String> stream =
                    Files.lines(
                            Paths.get(FileUtils.expandUserHome(delegationTokenFilePath)),
                            StandardCharsets.UTF_8)) {
                stream.forEach(s -> tokenBuilder.append(s));
            } catch (IOException e) {
                LOG.warn("Exception in reading or parsing delegation token file", e);
            }
            String delegationToken = tokenBuilder.toString();
            objectStorageBuilder.additionalClientConfigurator(
                    new DelegationTokenConfigurator(delegationToken));
        }

        if (!propertyAccessor.asBoolean().get(BmcProperties.OBJECT_AUTO_CLOSE_INPUT_STREAM)) {
            Options.shouldAutoCloseResponseInputStream(false);
        }
        // If a proxy is not defined, use the existing ObjectStorageClient that leverages the DefaultConnectorProvider.
        // Else, build an ObjectStorageClient that leverages the ApacheConnector to configure a proxy.
        return (StringUtils.isBlank(httpProxyUri))
                ? objectStorageBuilder.build(authDetailsProvider)
                : buildClientWithProxy(
                        authDetailsProvider, clientConfig, propertyAccessor, httpProxyUri);
    }

    private MonitoringClient buildMonitoringClientWithProxy(BasicAuthenticationDetailsProvider authDetailsProvider,
                                                            ClientConfiguration clientConfig,
                                                            BmcPropertyAccessor propertyAccessor,
                                                            String httpProxyUri,
                                                            MonitoringClient.Builder monitoringClientBuilder) {
        LOG.info("Setting Monitoring HTTP Proxy URI to {}", httpProxyUri);
        final ApacheConnectionPoolConfig poolConfig = buildApacheConnectionPoolingClientConfig(clientConfig);
        final ProxyConfiguration proxyConfig = buildApacheProxyConfig(httpProxyUri, propertyAccessor);

        com.oracle.bmc.http.client.jersey.apacheconfigurator.ApacheConnectorProperties
                apacheConnectorProperties =
                com.oracle.bmc.http.client.jersey.apacheconfigurator
                        .ApacheConnectorProperties.builder()
                        .connectionPoolConfig(poolConfig)
                        .build();

        final ClientConfigurator clientConnectorPropertiesConfigurator =
                new ClientConfiguratorWithProxy(
                        apacheConnectorProperties, proxyConfig, propertyAccessor);

        monitoringClientBuilder.clientConfigurator(clientConnectorPropertiesConfigurator);

        return monitoringClientBuilder.build(authDetailsProvider);
    }

    private ObjectStorage buildClientWithProxy(
            final BasicAuthenticationDetailsProvider authDetailsProvider,
            final ClientConfiguration clientConfig,
            final BmcPropertyAccessor propertyAccessor,
            final String httpProxyUri) {
        LOG.info("Setting HTTP Proxy URI to {}", httpProxyUri);
        final ApacheConnectionPoolConfig poolConfig =
                buildApacheConnectionPoolingClientConfig(clientConfig);
        final ProxyConfiguration proxyConfig =
                buildApacheProxyConfig(httpProxyUri, propertyAccessor);

        com.oracle.bmc.http.client.jersey.apacheconfigurator.ApacheConnectorProperties
                apacheConnectorProperties =
                        com.oracle.bmc.http.client.jersey.apacheconfigurator
                                .ApacheConnectorProperties.builder()
                                .connectionPoolConfig(poolConfig)
                                .build();

        final ClientConfigurator clientConnectorPropertiesConfigurator =
                new ClientConfiguratorWithProxy(
                        apacheConnectorProperties, proxyConfig, propertyAccessor);

        ObjectStorageClient.Builder objectStorageBuilder =
                ObjectStorageClient.builder()
                        .configuration(clientConfig)
                        .clientConfigurator(clientConnectorPropertiesConfigurator);

        return objectStorageBuilder.build(authDetailsProvider);
    }

    // A pool config is required as the default pool config for Hadoop versions > 2.8.x keeps connections alive
    // indefinitely causing the job to remain stuck in a RUNNING state.
    private ApacheConnectionPoolConfig buildApacheConnectionPoolingClientConfig(
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
        return poolConfig;
    }

    private ProxyConfiguration buildApacheProxyConfig(
            final String httpProxyUri, final BmcPropertyAccessor propertyAccessor) {
        URI uri = URI.create(httpProxyUri);
        final ProxyConfiguration proxyConfig =
                ProxyConfiguration.builder()
                        .proxy(
                                new Proxy(
                                        uri.getScheme().equalsIgnoreCase("http")
                                                ? Proxy.Type.HTTP
                                                : Proxy.Type.SOCKS,
                                        new InetSocketAddress(uri.getHost(), uri.getPort())))
                        .username(
                                propertyAccessor.asString().get(BmcProperties.HTTP_PROXY_USERNAME))
                        .password(
                                propertyAccessor
                                        .asString()
                                        .get(BmcProperties.HTTP_PROXY_PASSWORD)
                                        .toCharArray())
                        .build();
        return proxyConfig;
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

        if (propertyAccessor.asBoolean().get(BmcProperties.OBJECT_STORE_SESSION_AUTHENTICATOR_ENABLE)) {
            String regionId = propertyAccessor.asString().get(BmcProperties.REGION_CODE_OR_ID);
            if (regionId == null) {
                throw new IllegalArgumentException("Missing required property 'fs.oci.client.regionCodeOrId' when session authentication is enabled");
            }

            Region region = Region.fromRegionCodeOrId(regionId);

            try {
                return SessionTokenAuthenticationDetailsProvider.builder()
                        .fingerprint(fingerprint)
                        .region(region)
                        .tenantId(tenantId)
                        .privateKeyFilePath(pemFilePath)
                        .userId(userId)
                        .sessionTokenFilePath(propertyAccessor.asString().get(BmcProperties.SESSION_TOKEN_FILE_PATH))
                        .build();
            } catch (IOException e) {
                throw new RuntimeException("Failed to build SessionTokenAuthenticationDetailsProvider", e);
            }
        }

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
