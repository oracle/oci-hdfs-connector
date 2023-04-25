/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.util.function.Supplier;
import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BmcDirectFSInputStreamTest {
    @Mock private InputStream inputStream;

    @Mock private ObjectStorage objectStorage;

    @Mock private FileStatus fileStatus;

    private static final FileSystem.Statistics statistics = new FileSystem.Statistics("ocitest");
    private static final Random randomGenerator = new Random();

    private static final Supplier<GetObjectRequest.Builder> getObjectRequestBuilderSupplier =
            new Supplier<GetObjectRequest.Builder>() {
                @Override
                public GetObjectRequest.Builder get() {
                    return GetObjectRequest.builder();
                }
            };

    private static final int READ_COUNT = 100;

    @Test
    public void normalReads() throws IOException {
        when(inputStream.read()).thenReturn(generateRandomByte());
        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenReturn(GetObjectResponse.builder().inputStream(inputStream).build());

        BmcDirectFSInputStream directFSInputStream =
                new BmcDirectFSInputStream(
                        objectStorage, fileStatus, getObjectRequestBuilderSupplier, statistics);

        for (int i = 0; i < READ_COUNT; i++) {
            directFSInputStream.read();
        }

        // ObjectStorage#read should be invoked exactly once
        verify(objectStorage, times(1)).getObject(any(GetObjectRequest.class));
        // InputStream#read should be invoked READ_COUNT number of times
        verify(inputStream, times(READ_COUNT)).read();
    }

    @Test
    public void readsWithFailure() throws IOException {
        List<Integer> failurePositions = new ArrayList<>();
        // Assume the first read does not result in a failure
        int lastFailurePosition = 1;
        while (true) {
            // Consecutive failures not expected during reads
            lastFailurePosition +=
                    (2 + randomGenerator.nextInt(READ_COUNT - lastFailurePosition - 1));
            if (lastFailurePosition >= (READ_COUNT - 1)) {
                break;
            }
            failurePositions.add(lastFailurePosition);
        }

        // Since the first read is not a failure, return a random byte when read
        Stubber stubber = doReturn(generateRandomByte());
        for (int i = 1, j = 0; i < READ_COUNT; i++) {
            if (j < failurePositions.size() && i == failurePositions.get(j)) {
                // Throw an IOException if this was a failure position
                stubber = stubber.doThrow(new IOException("test exception"));
                j++;
            } else {
                // Else return a random byte
                stubber = stubber.doReturn(generateRandomByte());
            }
        }

        stubber.when(inputStream).read();

        ArgumentCaptor<GetObjectRequest> argumentCaptor =
                ArgumentCaptor.forClass(GetObjectRequest.class);
        when(objectStorage.getObject(argumentCaptor.capture()))
                .thenAnswer(
                        new Answer<GetObjectResponse>() {
                            @Override
                            public GetObjectResponse answer(InvocationOnMock invocationOnMock) {
                                final GetObjectRequest getObjectRequest =
                                        invocationOnMock.getArgumentAt(0, GetObjectRequest.class);
                                final Range contentRange =
                                        getObjectRequest.getRange() == null
                                                ? new Range(0L, null)
                                                : getObjectRequest.getRange();
                                return GetObjectResponse.builder()
                                        .inputStream(inputStream)
                                        .contentRange(contentRange)
                                        .eTag("dummy etag")
                                        .contentLength(0L)
                                        .build();
                            }
                        });

        final BmcDirectFSInputStream directFSInputStream =
                new BmcDirectFSInputStream(
                        objectStorage, fileStatus, getObjectRequestBuilderSupplier, statistics);

        for (int i = 0; i < READ_COUNT; i++) {
            directFSInputStream.read();
        }

        // ObjectStorage#read should be invoked 1 more than the count of items in the failurePositions list
        verify(objectStorage, times(1 + failurePositions.size()))
                .getObject(any(GetObjectRequest.class));
        // InputStream#read should be invoked again for all the failurePositions
        verify(inputStream, times(READ_COUNT + failurePositions.size())).read();

        assertEquals(1 + failurePositions.size(), argumentCaptor.getAllValues().size());
        for (int i = 0, counter = 0; i < argumentCaptor.getAllValues().size(); i++) {
            final Range range = argumentCaptor.getAllValues().get(i).getRange();
            if (i == 0) {
                assertEquals(null, range);
            } else {
                assertEquals(
                        failurePositions.get(i - 1) - counter++, range.getStartByte().intValue());
                assertEquals(null, range.getEndByte());
            }
        }
    }

    private static int generateRandomByte() {
        return randomGenerator.nextInt(256);
    }
}
