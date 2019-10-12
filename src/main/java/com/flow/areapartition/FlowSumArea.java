/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/7
 * Time: 11:42
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package com.flow.areapartition;

import com.flow.counter.FlowBean;
import com.flow.hdfsutil.HdfsUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class FlowSumArea extends Configured implements Tool {


    private static final Log logger = LogFactory.getLog(FlowSumArea.class);

    public static void main(String[] args) throws Exception {
        HdfsUtil hdfsUtil = new HdfsUtil();
        if (hdfsUtil.rmdir("/dst_Partition")) {
            logger.info("���Ŀ¼�ļ�ɾ���ɹ�");
        }
        int res = ToolRunner.run(new Configuration(), new FlowSumArea(), args);
        if (res == 0)
            hdfsUtil.listFiles("/dst_Partition", true);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //�����ļ�����jarͬ��Ҳ��ִ��
        //conf.set("mapreduce.job.jar","out\\artifacts\\comflowcounter_jar\\comflowcounter.jar");
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowSumArea.class);
        job.setJar("out\\artifacts\\comflowcounter_jar\\comflowcounter.jar");

        job.setMapperClass(FlowAreaMapper.class);
        job.setReducerClass(FlowAreaReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //�����Զ���ķ����߼�
        job.setPartitionerClass(AreaPartitioner.class);
        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job, new Path("/MRInput"));
        FileOutputFormat.setOutputPath(job, new Path("/dst_Partition"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
     *  ��ԭʼ��������ͳ�ƣ���ͬʡ���û�������ͳ�ƽ���������ͬʡ��
     *  mapʱ������ִ�з���
     *  ��Ҫ�Զ�����߼���
     *  1. ����������߼����Զ���һ��partitioner
     *  2. ����reducer task�Ĳ���������
     * */
    public static class FlowAreaMapper extends Mapper<LongWritable, Text, Text, FlowBean> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //��ȡ�����ֶη�װ��һ��FlowBean��
            String line = value.toString();
            String[] fields = StringUtils.split(line, ' ');
            String phoneNumber = fields[0];
            long upFlow = Long.parseLong(fields[1]);
            long downFlow = Long.parseLong(fields[2]);
            context.write(new Text(phoneNumber), new FlowBean(phoneNumber, upFlow, downFlow));
        }
    }

    public static class FlowAreaReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context)
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
}
