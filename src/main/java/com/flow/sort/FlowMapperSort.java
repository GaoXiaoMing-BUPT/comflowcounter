/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/7
 * Time: 9:19
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package com.flow.sort;

import com.flow.counter.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import scala.xml.Null;

import java.io.IOException;


public class FlowMapperSort extends Mapper<LongWritable, Text, FlowBean, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取各个字段封装成一个FlowBean，
        String line = value.toString();
        String[] fields = StringUtils.split(line, ' ');
        String phoneNumber = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        context.write(new FlowBean(phoneNumber, upFlow, downFlow), NullWritable.get());
    }
}
