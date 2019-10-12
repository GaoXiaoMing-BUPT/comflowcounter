package com.flow.sort;

import com.flow.counter.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import static org.junit.Assert.*;

public class FlowMapperSortTest {

    @Test
    public void map() {
        FlowBean flowBean = new FlowBean("13537865791", 12, 6);
        new MapDriver<LongWritable, Text, FlowBean, NullWritable>()
                .withMapper(new FlowMapperSort())
                .withInput(new LongWritable(0), new Text("13537865791 12 6"))
                .withOutput(flowBean, NullWritable.get());
    }
}