package com.flow.counter;

import com.flow.hdfsutil.HdfsUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;

/*
 * 新版本写法，
 * 实现 Tool的run方法
 * 继承 Configured
 * 要求把job部分写在一个run方法中
 * 调用ToolRunner静态方法run，然后传入参数进行运行
 * */
public class FlowRunner extends Configured implements Tool {
    private static final Log logger = LogFactory.getLog(FlowRunner.class);

    public static void main(String[] args) throws Exception {
        long startTime = new Date().getTime();
        HdfsUtil hdfsUtil = new HdfsUtil();
        if (hdfsUtil.rmdir(("/MROutput"))) {
            logger.info("delete MROutput success");
        }
        int res = ToolRunner.run(new Configuration(), new FlowRunner(), args);
        if (res == 0)
            hdfsUtil.printFileContent("/MROutput/part-r-00000", 10);
        logger.info("程序结束 日志测试");
        System.out.println("\n时长：" + (new Date().getTime() - startTime));
        System.exit(res);

    }

    /*public static void main(String[] args) throws Exception {//本地调试测试
        int res = ToolRunner.run(new FlowRunner(),args);
        System.exit(res);
    }*/
    /* 自己书写run方法 */
    @Override
    public int run(String[] args) throws Exception {//传入参数并解析

        Configuration conf = new Configuration();
        //conf.set("mapreduce.job.jar", "out\\artifacts\\comflowcounter_jar\\comflowcounter.jar");
        //conf.set("fs.defaultFS","file:///");//这两项用于本地调试
        //conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf);        //获取配置文件
        job.setUser("gaoxiaoming");
        job.setJar("out\\artifacts\\comflowcounter_jar\\comflowcounter.jar");
        job.setJarByClass(FlowRunner.class);//限定使用 job的类
        job.setJobName("单词计数");


        //设置map和reduce的类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //设置reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //设置map的输出类型
        job.setMapOutputValueClass(FlowBean.class);
        job.setMapOutputKeyClass(Text.class);
//        FileInputFormat.setInputPaths(job, new Path("H:\\Hadoop\\hadoop_local_data\\src"));//设置本地文件夹
//        FileOutputFormat.setOutputPath(job, new Path("H:\\Hadoop\\hadoop_local_data\\dst"));
        FileInputFormat.setInputPaths(job, new Path("/MRInput"));//集群hdfs路径
        FileOutputFormat.setOutputPath(job, new Path("/MROutput"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
