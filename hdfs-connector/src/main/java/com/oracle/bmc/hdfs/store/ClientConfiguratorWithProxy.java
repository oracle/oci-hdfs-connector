/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.http.client.HttpClientBuilder;
import com.oracle.bmc.http.client.ProxyConfiguration;
import com.oracle.bmc.http.client.StandardClientProperties;
import com.oracle.bmc.http.client.jersey.JerseyClientProperty;
import com.oracle.bmc.http.client.jersey.apacheconfigurator.ApacheConfigurator;
import com.oracle.bmc.http.client.jersey.apacheconfigurator.ApacheConnectorProperties;
import org.glassfish.jersey.logging.LoggingFeature;

import static org.glassfish.jersey.logging.LoggingFeature.LOGGING_FEATURE_LOGGER_LEVEL_CLIENT;
import static org.glassfish.jersey.logging.LoggingFeature.LOGGING_FEATURE_VERBOSITY_CLIENT;

public class ClientConfiguratorWithProxy extends ApacheConfigurator {

    ProxyConfiguration proxyConfiguration;
    BmcPropertyAccessor propertyAccessor;

    public ClientConfiguratorWithProxy(
            ApacheConnectorProperties apacheConnectorProperties,
            ProxyConfiguration proxyConfiguration,
            BmcPropertyAccessor propertyAccessor) {
        super(apacheConnectorProperties);
        this.proxyConfiguration = proxyConfiguration;
        this.propertyAccessor = propertyAccessor;
    }

    @Override
    public void customizeClient(HttpClientBuilder builder) {
        builder.property(StandardClientProperties.PROXY, proxyConfiguration);

        if (propertyAccessor.asBoolean().get(BmcProperties.JERSEY_CLIENT_LOGGING_ENABLED)) {
            builder.property(
                    JerseyClientProperty.create(LOGGING_FEATURE_VERBOSITY_CLIENT),
                    LoggingFeature.Verbosity.valueOf(
                            propertyAccessor
                                    .asString()
                                    .get(BmcProperties.JERSEY_CLIENT_LOGGING_VERBOSITY)));
            builder.property(
                    JerseyClientProperty.create(LOGGING_FEATURE_LOGGER_LEVEL_CLIENT),
                    propertyAccessor.asString().get(BmcProperties.JERSEY_CLIENT_LOGGING_LEVEL));
        }
        super.customizeClient(builder);
    }
}
