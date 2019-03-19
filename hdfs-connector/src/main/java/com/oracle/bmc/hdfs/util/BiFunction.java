/**
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.util;

/**
 * Similar to Java8 BiFunction.
 *
 * @param <ARG1>
 *            Type for first arg.
 * @param <ARG2>
 *            Type for second arg.
 * @param <OUTPUT>
 *            Type of output.
 */
public interface BiFunction<ARG1, ARG2, OUTPUT> {
    /**
     * Executes the function on the two given args.
     *
     * @param arg1
     *            The first arg.
     * @param arg2
     *            The second arg.
     * @return The result of executing the function.
     */
    OUTPUT apply(ARG1 arg1, ARG2 arg2);
}
