package com.oracle.bmc.hdfs.auth.spnego;

import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.*;

public class TokenRenewalServiceTest {

    @Test
    public void testGetInstanceReturnsSingleton() {
        TokenRenewalService first = TokenRenewalService.getInstance();
        TokenRenewalService second = TokenRenewalService.getInstance();

        assertSame(first, second);
    }

    @Test
    public void testRegisterSchedulesRefresh() {
        UPSTAuthenticationDetailsProvider provider = mock(UPSTAuthenticationDetailsProvider.class);
        when(provider.getTimeUntilRefresh()).thenReturn(10L, 1000000L);
        when(provider.refresh()).thenReturn("token");

        TokenRenewalService service = TokenRenewalService.getInstance();
        service.register(provider);

        verify(provider, timeout(1000).times(1)).refresh();
        verify(provider, atLeastOnce()).getTimeUntilRefresh();
    }

    @Test
    public void testRegisterReschedulesAfterRefresh() {
        UPSTAuthenticationDetailsProvider provider = mock(UPSTAuthenticationDetailsProvider.class);
        when(provider.getTimeUntilRefresh()).thenReturn(10L, 10L, 1000000L);
        when(provider.refresh()).thenReturn("token");

        TokenRenewalService service = TokenRenewalService.getInstance();
        service.register(provider);

        // verify refresh is called at least twice due to rescheduling
        verify(provider, timeout(2000).atLeast(2)).refresh();
        verify(provider, atLeast(3)).getTimeUntilRefresh();
    }
}
