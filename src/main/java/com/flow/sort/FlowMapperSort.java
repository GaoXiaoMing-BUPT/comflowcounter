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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;


public class FlowMapperSort extends Mapper<LongWritable, Text, FlowBean, NullWritable> {
    private Log logger = LogFactory.getLog(FlowMapperSort.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取各个字段封装成一个FlowBean，

        String line = value.toString();
        String[] fields = StringUtils.split(line, ' ');
        if (fields.length != 3) {
            context.getCounter(UserDataEnum.MISSING).increment(1L);
        } else {
            String phoneNumber = fields[0];
            try {
                long upFlow = Long.parseLong(fields[1]);
                long downFlow = Long.parseLong(fields[2]);
                context.write(new FlowBean(phoneNumber, upFlow, downFlow), NullWritable.get());
                if (phoneNumber.length() != 11)
                    throw new NumberFormatException();
                context.getCounter(UserDataEnum.NORMAL).increment(1L);
            } catch (NumberFormatException e) {
                context.getCounter(UserDataEnum.ERRORData).increment(1L);

            }

        }


    }

    enum UserDataEnum {
        MISSING,
        NORMAL,
        ERRORData
    }
}
