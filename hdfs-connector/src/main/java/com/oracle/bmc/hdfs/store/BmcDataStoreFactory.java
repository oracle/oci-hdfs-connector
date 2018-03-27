/**
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.store;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;

import com.google.common.base.Supplier;
import com.oracle.bmc.ClientConfiguration;
import com.oracle.bmc.ClientRuntime;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider;
import com.oracle.bmc.auth.SimplePrivateKeySupplier;
import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.util.StreamUtils;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory class to create a {@link BmcDataStore}. This factory allows for the usage of custom classes to
 * communicate/authenticate with Object Store, else falling back to defaults.
 */
@Slf4j
@RequiredArgsConstructor
public class BmcDataStoreFactory {
    private static final String OCI_PROPERTIES_FILE_NAME = "oci.properties";
    private final Configuration configuration;

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
        // so overriding property foobar is done by specifing foobar.bucket.namespace
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

    private ObjectStorage createClient(final BmcPropertyAccessor propertyAccessor) {
        final String customObjectStoreClient =
                propertyAccessor.asString().get(BmcProperties.OBJECT_STORE_CLIENT_CLASS);
        final ObjectStorage objectStorage;
        if (customObjectStoreClient != null) {
            objectStorage = this.createClass(customObjectStoreClient);
        } else {
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

            objectStorage =
                    new ObjectStorageClient(
                            this.createAuthenticator(propertyAccessor),
                            clientConfigurationBuilder.build());
        }

        final String endpoint = propertyAccessor.asString().get(BmcProperties.HOST_NAME);
        LOG.info("Using endpoint {}", endpoint);
        objectStorage.setEndpoint(endpoint);

        return objectStorage;
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

    private BasicAuthenticationDetailsProvider createAuthenticator(
            BmcPropertyAccessor propertyAccessor) {
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
