package com.flow.counter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    /*
     * 框架每传输一组数据<FlowBean1,FlowBean2,FlowBean3,...>,即每一个key的数据
     * 然后对相同的手机号码进行累计求和，传递给context输出
     * */
    @Override
    public void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {
        long upFlowCounter = 0;
        long downFlowCounter = 0;

        for (FlowBean flowBean : values) {
            upFlowCounter += flowBean.getUpFlow();
            downFlowCounter += flowBean.getDownFlow();
        }
        context.write(key, new FlowBean(key.toString(), upFlowCounter, downFlowCounter));
    }

}
