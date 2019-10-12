/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/7
 * Time: 9:20
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package com.flow.sort;

import com.flow.counter.FlowBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class FlowReducerSort extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {
    private MultipleOutputs<FlowBean, NullWritable> multipleOutputs;

    @Override
    protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //context.write(key, NullWritable.get());
        String filename = key.getPhoneNumber().substring(0, 4);
        multipleOutputs.write(filename, key, NullWritable.get());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<FlowBean, NullWritable>(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
