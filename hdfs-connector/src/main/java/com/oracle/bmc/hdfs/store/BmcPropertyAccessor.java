/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import com.oracle.bmc.hdfs.BmcConstants;

import java.io.IOException;
import java.util.function.Function;

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
public class BmcPropertyAccessor {
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

    public Accessor<Boolean> asBoolean() {
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

    public Accessor<Integer> asInteger() {
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

    public Accessor<String> asString() {
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

    public Accessor<char[]> asPassword() {
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

    public Accessor<Long> asLong() {
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
    public interface Accessor<T> {
        /**
         * Get the value of this property, interpreted as type T. If the property was not configured, the default
         * value ({@link BmcProperties#getDefaultValue()} will be returned.
         *
         * @param property
         *            The BMC property.
         * @return
         */
        T get(BmcProperties property);

        /**
         * If the value of the property, interpreted as type T, is non-null, apply the function to that value, and
         * return the result of the function. IF the value is null, return null.
         * @param property The BMC property
         * @param fn the function to apply to the non-null value
         * @param <R> return type
         * @return result of applying the function to the non-null value, or null if the value is null
         */
        default <R> R forNonNull(BmcProperties property, Function<T, R> fn) {
            T value = get(property);
            if (value != null) {
                return fn.apply(value);
            } else {
                return null;
            }
        }
    }
}
