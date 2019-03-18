/**
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.store;

import com.oracle.bmc.hdfs.BmcConstants;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.oracle.bmc.hdfs.BmcProperties;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Wrapper around hadoop configuration that allows you to use the default value of a OCI property (which may be null),
 * vs standard hadoop configuration which doesn't allow null values.
 */
@RequiredArgsConstructor
@Slf4j
class BmcPropertyAccessor {
    private final Configuration configuration;
    private final String propertyOverrideSuffix;

    /**
     * Get the string value associated with this property.
     *
     * @param property
     *            The property name.
     * @return The value, or null.
     */
    String get(String property) {
        return configuration.get(property);
    }

    private String getOverriddenPropertyName(BmcProperties property) {
        return property.getPropertyName() + propertyOverrideSuffix;
    }

    Accessor<Boolean> asBoolean() {
        return new Accessor<Boolean>() {
            @Override
            public Boolean get(BmcProperties property) {
                // overridden properties only use the new property names, no need to check for clashes
                String overriddenPropertyName = getOverriddenPropertyName(property);
                if (exists(overriddenPropertyName)) {
                    LOG.debug("Using key {}", overriddenPropertyName);
                    return configuration.getBoolean(overriddenPropertyName, false);
                }

                checkClash(property);
                if (exists(property.getPropertyName())) {
                    LOG.debug("Using key {}", property.getPropertyName());
                    return configuration.getBoolean(property.getPropertyName(), false);
                }
                String deprecatedKey =
                        BmcConstants.Deprecated.getDeprecatedKey(property.getPropertyName());
                if (exists(deprecatedKey)) {
                    LOG.debug("Using key {}", deprecatedKey);
                    return configuration.getBoolean(deprecatedKey, false);
                }

                LOG.debug("No configuration found for {}", property.getPropertyName());
                return (Boolean) property.getDefaultValue();
            }
        };
    }

    Accessor<Integer> asInteger() {
        return new Accessor<Integer>() {
            @Override
            public Integer get(BmcProperties property) {
                // overridden properties only use the new property names, no need to check for clashes
                String overriddenPropertyName = getOverriddenPropertyName(property);
                if (exists(overriddenPropertyName)) {
                    LOG.debug("Using key {}", overriddenPropertyName);
                    return configuration.getInt(overriddenPropertyName, 0);
                }

                checkClash(property);
                if (exists(property.getPropertyName())) {
                    LOG.debug("Using key {}", property.getPropertyName());
                    return configuration.getInt(property.getPropertyName(), 0);
                }
                String deprecatedKey =
                        BmcConstants.Deprecated.getDeprecatedKey(property.getPropertyName());
                if (exists(deprecatedKey)) {
                    LOG.debug("Using key {}", deprecatedKey);
                    return configuration.getInt(deprecatedKey, 0);
                }

                LOG.debug("No configuration found for {}", property.getPropertyName());
                return (Integer) property.getDefaultValue();
            }
        };
    }

    Accessor<String> asString() {
        return new Accessor<String>() {
            @Override
            public String get(BmcProperties property) {
                // overridden properties only use the new property names, no need to check for clashes
                String overriddenPropertyName = getOverriddenPropertyName(property);
                if (exists(overriddenPropertyName)) {
                    LOG.debug("Using key {}", overriddenPropertyName);
                    return configuration.getTrimmed(overriddenPropertyName, null);
                }

                checkClash(property);
                if (exists(property.getPropertyName())) {
                    LOG.debug("Using key {}", property.getPropertyName());
                    return configuration.getTrimmed(property.getPropertyName(), null);
                }
                String deprecatedKey =
                        BmcConstants.Deprecated.getDeprecatedKey(property.getPropertyName());
                if (exists(deprecatedKey)) {
                    LOG.debug("Using key {}", deprecatedKey);
                    return configuration.getTrimmed(deprecatedKey, null);
                }

                LOG.debug("No configuration found for {}", property.getPropertyName());
                return (String) property.getDefaultValue();
            }
        };
    }

    Accessor<char[]> asPassword() {
        return new Accessor<char[]>() {
            @Override
            public char[] get(BmcProperties property) {
                // we manually check for clashes under both old and new keys for passwords
                // as the normal clashes call won't work with passwords
                try {
                    // overridden properties only use the new property names, no need to check for clashes
                    String overriddenPropertyName = getOverriddenPropertyName(property);
                    char[] overriddenPassword = configuration.getPassword(overriddenPropertyName);
                    if (overriddenPassword != null) {
                        LOG.debug("Using key {}", overriddenPropertyName);
                        return overriddenPassword;
                    }

                    char[] password = configuration.getPassword(property.getPropertyName());

                    String deprecatedKey =
                            BmcConstants.Deprecated.getDeprecatedKey(property.getPropertyName());
                    char[] deprecatedPassword = configuration.getPassword(deprecatedKey);

                    if (password != null && deprecatedPassword != null) {
                        throwClash(property);
                    }

                    if (password != null) {
                        LOG.debug("Using key {}", property.getPropertyName());
                        return password;
                    }
                    if (deprecatedPassword != null) {
                        LOG.debug("Using key {}", deprecatedKey);
                        return deprecatedPassword;
                    }

                    // there's never a default password, so always return null
                    LOG.debug("No configuration found for {}", property.getPropertyName());
                    return null;
                } catch (IOException e) {
                    throw new IllegalArgumentException("Unable to extract password", e);
                }
            }
        };
    }

    Accessor<Long> asLong() {
        return new Accessor<Long>() {
            @Override
            public Long get(BmcProperties property) {
                // overridden properties only use the new property names, no need to check for clashes
                String overriddenPropertyName = getOverriddenPropertyName(property);
                if (exists(overriddenPropertyName)) {
                    LOG.debug("Using key {}", overriddenPropertyName);
                    return configuration.getLong(overriddenPropertyName, 0L);
                }

                checkClash(property);
                if (exists(property.getPropertyName())) {
                    LOG.debug("Using key {}", property.getPropertyName());
                    return configuration.getLong(property.getPropertyName(), 0L);
                }
                String deprecatedKey =
                        BmcConstants.Deprecated.getDeprecatedKey(property.getPropertyName());
                if (exists(deprecatedKey)) {
                    LOG.debug("Using key {}", deprecatedKey);
                    return configuration.getLong(deprecatedKey, 0L);
                }

                LOG.debug("No configuration found for {}", property.getPropertyName());
                return (Long) property.getDefaultValue();
            }
        };
    }

    private boolean exists(String key) {
        return this.get(key) != null;
    }

    private void checkClash(BmcProperties property) {
        if (clashes(property)) {
            throwClash(property);
        }
    }

    private void throwClash(BmcProperties property) {
        throw new IllegalArgumentException(
                "Configuration uses both "
                        + property.getPropertyName()
                        + " and the deprecated key "
                        + BmcConstants.Deprecated.getDeprecatedKey(property.getPropertyName())
                        + ". Use one or the other, but not both.");
    }

    /**
     * Return true if both the new key and the deprecated key are used
     *
     * @param property
     *            property to check in configuration
     * @return true if the property is defined both using the new and the deprecated key
     */
    private boolean clashes(BmcProperties property) {
        return this.get(property.getPropertyName()) != null
                && this.get(BmcConstants.Deprecated.getDeprecatedKey(property.getPropertyName()))
                        != null;
    }

    /**
     * An accessor for some configuration object.
     *
     * @param <T>
     *            The type of values it returns.
     */
    interface Accessor<T> {
        /**
         * Get the value of this property, interpreted as type <T>. If the property was not configured, the default
         * value ({@link BmcProperties#getDefaultValue()} will be returned.
         *
         * @param property
         *            The BMC property.
         * @return
         */
        T get(BmcProperties property);
    }
}
