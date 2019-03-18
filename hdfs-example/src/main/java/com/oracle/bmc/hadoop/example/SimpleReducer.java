/**
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hadoop.example;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimpleReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable result = new IntWritable();

    @Override
    public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (final IntWritable val : values) {
            sum += val.get();
        }
        this.result.set(sum);
        context.write(key, this.result);
    }
}
