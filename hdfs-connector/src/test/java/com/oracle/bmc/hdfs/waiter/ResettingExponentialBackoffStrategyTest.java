/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.waiter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.when;

import com.oracle.bmc.waiter.WaiterConfiguration;
import org.junit.Assert;
import org.junit.Test;

import com.oracle.bmc.waiter.WaiterConfiguration.WaitContext;
import org.mockito.Mockito;

public class ResettingExponentialBackoffStrategyTest {

    @Test
    public void getDelays() {
        ResettingExponentialBackoffStrategy strategy = new ResettingExponentialBackoffStrategy(30L);
        WaitContext wc = Mockito.mock(WaitContext.class);
        when(wc.getAttemptsMade())
                .thenReturn(0)
                .thenReturn(1)
                .thenReturn(2)
                .thenReturn(3)
                .thenReturn(4)
                .thenReturn(5)
                .thenReturn(6)
                .thenReturn(7)
                .thenReturn(8)
                .thenReturn(9)
                .thenReturn(10)
                .thenReturn(11);
        assertEquals(1000L, strategy.nextDelay(wc));
        assertEquals(2000L, strategy.nextDelay(wc));
        assertEquals(4000L, strategy.nextDelay(wc));
        assertEquals(8000L, strategy.nextDelay(wc));
        assertEquals(16000L, strategy.nextDelay(wc));
        assertEquals(1000L, strategy.nextDelay(wc));
        assertEquals(2000L, strategy.nextDelay(wc));
        assertEquals(4000L, strategy.nextDelay(wc));
        assertEquals(8000L, strategy.nextDelay(wc));
        assertEquals(16000L, strategy.nextDelay(wc));
        assertEquals(1000L, strategy.nextDelay(wc));

        strategy = new ResettingExponentialBackoffStrategy(30L, 10);
        when(wc.getAttemptsMade())
                .thenReturn(0)
                .thenReturn(1)
                .thenReturn(2)
                .thenReturn(3)
                .thenReturn(4)
                .thenReturn(5)
                .thenReturn(6)
                .thenReturn(7)
                .thenReturn(8)
                .thenReturn(9)
                .thenReturn(10)
                .thenReturn(11);
        long delay = strategy.nextDelay(wc);
        Assert.assertTrue(delay >= 1000L - 10L);
        Assert.assertTrue(delay < 1000L + 10L);
        delay = strategy.nextDelay(wc);
        Assert.assertTrue(delay >= 2000L - 10L);
        Assert.assertTrue(delay < 2000L + 10L);
        delay = strategy.nextDelay(wc);
        Assert.assertTrue(delay >= 4000L - 10L);
        Assert.assertTrue(delay < 4000L + 10L);
        delay = strategy.nextDelay(wc);
        Assert.assertTrue(delay >= 8000L - 10L);
        Assert.assertTrue(delay < 8000L + 10L);
        delay = strategy.nextDelay(wc);
        Assert.assertTrue(delay >= 16000L - 10L);
        Assert.assertTrue(delay < 16000L + 10L);
        delay = strategy.nextDelay(wc);
        Assert.assertTrue(delay >= 1000L - 10L);
        Assert.assertTrue(delay < 1000L + 10L);
        delay = strategy.nextDelay(wc);
        Assert.assertTrue(delay >= 2000L - 10L);
        Assert.assertTrue(delay < 2000L + 10L);
        delay = strategy.nextDelay(wc);
        Assert.assertTrue(delay >= 4000L - 10L);
        Assert.assertTrue(delay < 4000L + 10L);
        delay = strategy.nextDelay(wc);
        Assert.assertTrue(delay >= 8000L - 10L);
        Assert.assertTrue(delay < 8000L + 10L);
        delay = strategy.nextDelay(wc);
        Assert.assertTrue(delay >= 16000L - 10L);
        Assert.assertTrue(delay < 16000L + 10L);
        delay = strategy.nextDelay(wc);
        Assert.assertTrue(delay >= 1000L - 10L);
        Assert.assertTrue(delay < 1000L + 10L);
    }
}
