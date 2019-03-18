/**
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.store;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.oracle.bmc.hdfs.BmcConstants;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.oracle.bmc.hdfs.BmcProperties;

public class BmcPropertyAccessorTest {
    private static final String PROPERTY_OVERRIDE_SUFFIX = ".bucket.namespace";
    private Configuration configuration;
    private BmcPropertyAccessor propertyAccessor;

    @Before
    public void setUp() {
        this.configuration = new Configuration();
        this.propertyAccessor =
                new BmcPropertyAccessor(this.configuration, PROPERTY_OVERRIDE_SUFFIX);
    }

    @Test
    public void asString() {
        assertThat(propertyAccessor.asString().get(BmcProperties.HOST_NAME), nullValue());
        this.configuration.set(BmcProperties.HOST_NAME.getPropertyName(), "value");
        assertThat(propertyAccessor.asString().get(BmcProperties.HOST_NAME), is("value"));
        this.configuration.set(
                BmcProperties.HOST_NAME.getPropertyName() + PROPERTY_OVERRIDE_SUFFIX, "value2");
        assertThat(propertyAccessor.asString().get(BmcProperties.HOST_NAME), is("value2"));
    }

    @Test
    public void asLong() {
        assertThat(propertyAccessor.asLong().get(BmcProperties.BLOCK_SIZE_IN_MB), is(32L));
        this.configuration.setLong(BmcProperties.BLOCK_SIZE_IN_MB.getPropertyName(), 50L);
        assertThat(propertyAccessor.asLong().get(BmcProperties.BLOCK_SIZE_IN_MB), is(50L));
        this.configuration.setLong(
                BmcProperties.BLOCK_SIZE_IN_MB.getPropertyName() + PROPERTY_OVERRIDE_SUFFIX, 100L);
        assertThat(propertyAccessor.asLong().get(BmcProperties.BLOCK_SIZE_IN_MB), is(100L));
    }

    @Test
    public void asInteger() {
        assertThat(
                propertyAccessor.asInteger().get(BmcProperties.READ_TIMEOUT_MILLIS), nullValue());
        this.configuration.setInt(BmcProperties.READ_TIMEOUT_MILLIS.getPropertyName(), 25);
        assertThat(propertyAccessor.asInteger().get(BmcProperties.READ_TIMEOUT_MILLIS), is(25));
        this.configuration.setInt(
                BmcProperties.READ_TIMEOUT_MILLIS.getPropertyName() + PROPERTY_OVERRIDE_SUFFIX, 50);
        assertThat(propertyAccessor.asInteger().get(BmcProperties.READ_TIMEOUT_MILLIS), is(50));
    }

    @Test
    public void asBoolean() {
        assertThat(
                propertyAccessor.asBoolean().get(BmcProperties.IN_MEMORY_READ_BUFFER), is(false));
        this.configuration.setBoolean(BmcProperties.IN_MEMORY_READ_BUFFER.getPropertyName(), true);
        assertThat(propertyAccessor.asBoolean().get(BmcProperties.IN_MEMORY_READ_BUFFER), is(true));
        this.configuration.setBoolean(
                BmcProperties.IN_MEMORY_READ_BUFFER.getPropertyName() + PROPERTY_OVERRIDE_SUFFIX,
                false);
        assertThat(
                propertyAccessor.asBoolean().get(BmcProperties.IN_MEMORY_READ_BUFFER), is(false));
    }

    @Test
    public void asPassword() {
        assertThat(propertyAccessor.asPassword().get(BmcProperties.PASS_PHRASE), nullValue());
        this.configuration.set(BmcProperties.PASS_PHRASE.getPropertyName(), "pass");
        assertThat(
                propertyAccessor.asPassword().get(BmcProperties.PASS_PHRASE),
                is(new char[] {'p', 'a', 's', 's'}));
        this.configuration.set(
                BmcProperties.PASS_PHRASE.getPropertyName() + PROPERTY_OVERRIDE_SUFFIX, "ssap");
        assertThat(
                propertyAccessor.asPassword().get(BmcProperties.PASS_PHRASE),
                is(new char[] {'s', 's', 'a', 'p'}));
    }

    // Deprecated

    @Test
    public void asStringDeprecated() {
        this.configuration.set(
                BmcConstants.Deprecated.getDeprecatedKey(BmcProperties.HOST_NAME.getPropertyName()),
                "value");
        assertThat(propertyAccessor.asString().get(BmcProperties.HOST_NAME), is("value"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void asStringDeprecated_clashes() {
        this.configuration.set(
                BmcConstants.Deprecated.getDeprecatedKey(BmcProperties.HOST_NAME.getPropertyName()),
                "value");
        this.configuration.set(BmcProperties.HOST_NAME.getPropertyName(), "value");

        propertyAccessor.asString().get(BmcProperties.HOST_NAME);
    }

    @Test
    public void asLongDeprecated() {
        this.configuration.setLong(
                BmcConstants.Deprecated.getDeprecatedKey(
                        BmcProperties.BLOCK_SIZE_IN_MB.getPropertyName()),
                50L);
        assertThat(propertyAccessor.asLong().get(BmcProperties.BLOCK_SIZE_IN_MB), is(50L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void asLongDeprecated_clashes() {
        this.configuration.setLong(
                BmcConstants.Deprecated.getDeprecatedKey(
                        BmcProperties.BLOCK_SIZE_IN_MB.getPropertyName()),
                50L);
        this.configuration.setLong(BmcProperties.BLOCK_SIZE_IN_MB.getPropertyName(), 50L);

        propertyAccessor.asLong().get(BmcProperties.BLOCK_SIZE_IN_MB);
    }

    @Test
    public void asIntegerDeprecated() {
        this.configuration.setInt(
                BmcConstants.Deprecated.getDeprecatedKey(
                        BmcProperties.READ_TIMEOUT_MILLIS.getPropertyName()),
                25);
        assertThat(propertyAccessor.asInteger().get(BmcProperties.READ_TIMEOUT_MILLIS), is(25));
    }

    @Test(expected = IllegalArgumentException.class)
    public void asIntegerDeprecated_clashes() {
        this.configuration.setInt(
                BmcConstants.Deprecated.getDeprecatedKey(
                        BmcProperties.READ_TIMEOUT_MILLIS.getPropertyName()),
                25);
        this.configuration.setInt(BmcProperties.READ_TIMEOUT_MILLIS.getPropertyName(), 25);

        propertyAccessor.asInteger().get(BmcProperties.READ_TIMEOUT_MILLIS);
    }

    @Test
    public void asBooleanDeprecated() {
        this.configuration.setBoolean(
                BmcConstants.Deprecated.getDeprecatedKey(
                        BmcProperties.IN_MEMORY_READ_BUFFER.getPropertyName()),
                true);
        assertThat(propertyAccessor.asBoolean().get(BmcProperties.IN_MEMORY_READ_BUFFER), is(true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void asBooleanDeprecated_clashes() {
        this.configuration.setBoolean(
                BmcConstants.Deprecated.getDeprecatedKey(
                        BmcProperties.IN_MEMORY_READ_BUFFER.getPropertyName()),
                true);
        this.configuration.setBoolean(BmcProperties.IN_MEMORY_READ_BUFFER.getPropertyName(), true);

        propertyAccessor.asBoolean().get(BmcProperties.IN_MEMORY_READ_BUFFER);
    }

    @Test
    public void asPasswordDeprecated() {
        this.configuration.set(
                BmcConstants.Deprecated.getDeprecatedKey(
                        BmcProperties.PASS_PHRASE.getPropertyName()),
                "pass");
        assertThat(
                propertyAccessor.asPassword().get(BmcProperties.PASS_PHRASE),
                is(new char[] {'p', 'a', 's', 's'}));
    }

    @Test(expected = IllegalArgumentException.class)
    public void asPasswordDeprecated_clashes() {
        this.configuration.set(
                BmcConstants.Deprecated.getDeprecatedKey(
                        BmcProperties.PASS_PHRASE.getPropertyName()),
                "pass");
        this.configuration.set(BmcProperties.PASS_PHRASE.getPropertyName(), "pass");

        propertyAccessor.asPassword().get(BmcProperties.PASS_PHRASE);
    }
}
