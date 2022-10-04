package com.oracle.bmc.hdfs.store;

import java.util.function.Supplier;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

/**
 * {@link BmcFSInputStream} abstract class for custom read stream for users to configure
 * their own streams. Since the constructor is invoked using reflection, subclasses are not allowed
 * to modify the constructor
 */
public abstract class AbstractBmcCustomFSInputStream extends BmcFSInputStream {

    protected final BmcPropertyAccessor propertyAccessor;

    public AbstractBmcCustomFSInputStream(
            BmcPropertyAccessor propertyAccessor,
            ObjectStorage objectStorage,
            FileStatus status,
            Supplier<GetObjectRequest.Builder> requestBuilder,
            FileSystem.Statistics statistics) {
        super(objectStorage, status, requestBuilder, statistics);
        this.propertyAccessor = propertyAccessor;
    }
}
