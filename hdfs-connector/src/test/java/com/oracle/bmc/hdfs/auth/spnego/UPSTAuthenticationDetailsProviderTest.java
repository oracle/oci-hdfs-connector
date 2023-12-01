package com.oracle.bmc.hdfs.auth.spnego;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

public class UPSTAuthenticationDetailsProviderTest {

    private UPSTAuthenticationDetailsProvider provider;
    private Configuration configuration;
    private UPSTManagerFactory upstManagerFactory;
    private UPSTManager upstManager;

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        upstManagerFactory = mock(UPSTManagerFactory.class);
        upstManager = mock(UPSTManager.class);

        when(upstManagerFactory.createUPSTManager(anyInt())).thenReturn(upstManager);

        provider = new UPSTAuthenticationDetailsProvider(configuration, upstManagerFactory);
    }

    @Test
    public void testRefresh() throws Exception {
        String expectedSpnegoToken = "mocked_spnego_token";
        String expectedUPSTToken = "mocked_upst_token";
        UPSTResponse mockedResponse = new UPSTResponse(expectedUPSTToken, null, "sample_exp");

        when(upstManager.generateSpnegoToken()).thenReturn(expectedSpnegoToken);
        when(upstManager.getUPSTToken(expectedSpnegoToken)).thenReturn(mockedResponse);

        String actualUPSTToken = provider.refresh();

        assertEquals(expectedUPSTToken, actualUPSTToken);
    }

    @Test
    public void testRefreshWhenSpnegoTokenGenerationFails() throws Exception {
        when(upstManager.generateSpnegoToken()).thenThrow(new RuntimeException("Failed to generate SPNEGO token"));
        String result = provider.refresh();
        assertNull(result);
    }

}
