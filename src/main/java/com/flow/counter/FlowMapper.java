package com.flow.counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/*
 * FlowBean,为自定义数据，必须符合hadoop序列化机制，则必须实现其序列化接口即
 *
 * */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    /*
     * 拿到日志中方的一行数据切分各个字段，抽取出需要的字段
     * 手机号，上、下行流量
     * 封装成key-value发送
     * */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] field = StringUtils.split(line, ' ');
        /*
         * 获取一行数据根据，将切割的字段进行取值封装数据
         * */
        String phoneNumber = field[0];
        long upFlow = Long.parseLong(field[1]);
        long downFlow = Long.parseLong(field[2]);
        FlowBean flowBean = new FlowBean(phoneNumber, upFlow, downFlow);
        context.write(new Text(phoneNumber), flowBean);//写入context，修改地址值
    }

}
