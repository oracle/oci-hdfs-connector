package com.oracle.bmc.hdfs.auth.spnego;

import org.apache.hadoop.conf.Configuration;
import java.io.IOException;

/**
 * Interface for generating tokens.
 */
public interface TokenGenerator {
    String generateToken( ) throws IOException, InterruptedException;
}
