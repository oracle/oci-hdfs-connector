/**
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.store;

import com.oracle.bmc.ClientConfiguration;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.http.ApacheConfigurator;
import com.oracle.bmc.http.ClientConfigurator;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BmcDataStoreFactory.class, ObjectStorageClient.class})
public class BmcDataStoreFactoryTest {

    @Mock private Configuration mockConfiguration;
    @Mock private BmcPropertyAccessor mockPropAccessor;
    @Mock private BmcPropertyAccessor.Accessor<String> mockStringAccessor;
    @Mock private BmcPropertyAccessor.Accessor<Integer> mockIntegerAccessor;
    @Mock private BmcPropertyAccessor.Accessor<Long> mockLongAccessor;
    @Mock private ObjectStorageClient mockObjectStorageClient;
    @Mock private ObjectStorageClient.Builder mockObjectStorageClientBuilder;

    private BmcDataStoreFactory factoryUnderTest;

    @Before
    public void setUp() {
        // Setup mockStringAccessor
        when(mockStringAccessor.get(eq(BmcProperties.OBJECT_STORE_CLIENT_CLASS))).thenReturn(null);
        when(mockStringAccessor.get(eq(BmcProperties.HOST_NAME))).thenReturn("some_hostname");

        // Setup mockIntegerAccessor
        when(mockIntegerAccessor.get(eq(BmcProperties.CONNECTION_TIMEOUT_MILLIS))).thenReturn(null);
        when(mockIntegerAccessor.get(eq(BmcProperties.READ_TIMEOUT_MILLIS))).thenReturn(null);

        // Setup mockLongAccessor
        when(mockLongAccessor.get(eq(BmcProperties.RETRY_TIMEOUT_IN_SECONDS))).thenReturn(30L);

        when(mockPropAccessor.asString()).thenReturn(mockStringAccessor);
        when(mockPropAccessor.asInteger()).thenReturn(mockIntegerAccessor);
        when(mockPropAccessor.asLong()).thenReturn(mockLongAccessor);

        // Allow stubbing of other methods inside the factory
        factoryUnderTest = spy(new BmcDataStoreFactory(mockConfiguration));
    }

    @Test
    public void createClient_withNoProxy_shouldUseObjectStorageClientConstructor()
            throws Exception {
        setUpStubForCreateAuthenticator();
        setUpObjectStorageClientBuilder();
        whenNew(ObjectStorageClient.class).withAnyArguments().thenReturn(mockObjectStorageClient);

        final ObjectStorage client = factoryUnderTest.createClient(mockPropAccessor);

        assertEquals("ObjectStorage should be equal", mockObjectStorageClient, client);
        verifyNew(ObjectStorageClient.class)
                .withArguments(
                        isA(BasicAuthenticationDetailsProvider.class),
                        isA(ClientConfiguration.class));
    }

    @Test
    public void createClient_withProxy_shouldUseClientBuilderWithApacheConfigurator()
            throws Exception {
        setUpStubForCreateAuthenticator();
        setUpObjectStorageClientBuilder();
        when(mockStringAccessor.get(eq(BmcProperties.HTTP_PROXY_URI)))
                .thenReturn("http://proxy.com:8080");
        when(mockStringAccessor.get(eq(BmcProperties.HTTP_PROXY_USERNAME))).thenReturn("username");
        when(mockStringAccessor.get(eq(BmcProperties.HTTP_PROXY_PASSWORD))).thenReturn("password");

        final ObjectStorage client = factoryUnderTest.createClient(mockPropAccessor);

        assertEquals("ObjectStorage should be equal", mockObjectStorageClient, client);

        verifyStatic(ObjectStorageClient.class);
        ObjectStorageClient.builder();
        verify(mockObjectStorageClientBuilder).configuration(isA(ClientConfiguration.class));

        final ArgumentCaptor<ClientConfigurator> configuratorCaptor =
                ArgumentCaptor.forClass(ClientConfigurator.class);
        verify(mockObjectStorageClientBuilder).clientConfigurator(configuratorCaptor.capture());

        final ClientConfigurator actualClientConfigurator = configuratorCaptor.getValue();
        assertTrue(
                "ClientConfigurator should be of type ApacheConfigurator",
                actualClientConfigurator instanceof ApacheConfigurator);
        verify(mockObjectStorageClientBuilder).build(isA(BasicAuthenticationDetailsProvider.class));
    }

    private void setUpStubForCreateAuthenticator() {
        final BasicAuthenticationDetailsProvider mockAuthProvider =
                mock(BasicAuthenticationDetailsProvider.class);
        doReturn(mockAuthProvider)
                .when(factoryUnderTest)
                .createAuthenticator(any(BmcPropertyAccessor.class));
    }

    private void setUpObjectStorageClientBuilder() {
        mockStatic(ObjectStorageClient.class);
        when(ObjectStorageClient.builder()).thenReturn(mockObjectStorageClientBuilder);

        when(mockObjectStorageClientBuilder.configuration(any(ClientConfiguration.class)))
                .thenReturn(mockObjectStorageClientBuilder);
        when(mockObjectStorageClientBuilder.clientConfigurator((any(ClientConfigurator.class))))
                .thenReturn(mockObjectStorageClientBuilder);
        when(mockObjectStorageClientBuilder.build(any(AuthenticationDetailsProvider.class)))
                .thenReturn(mockObjectStorageClient);
    }
}
