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
import com.flow.hdfsutil.HdfsUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlowSortRunner extends Configured implements Tool {
    private static final Log logger = LogFactory.getLog(FlowSortRunner.class);

    public static void main(String[] args) throws Exception {
        HdfsUtil hdfsUtil = new HdfsUtil();
        if (hdfsUtil.rmdir(("hdfs://hadoop51:9000/SortOutput"))) {
            logger.info("delete SortOutput success");
        }

        int res = ToolRunner.run(new Configuration(), new FlowSortRunner(), args);
        if (res == 0)
            hdfsUtil.printFileContent("hdfs://hadoop51:9000/SortOutput/part-r-00000", 10);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // configuration.set("mapreduce.job.jar", "out\\artifacts\\comflowcounter_jar\\comflowcounter.jar");
        configuration.set("mapreduce.multipleoutputs", "135");
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FlowSortRunner.class);
        job.setJar("out\\artifacts\\comflowcounter_jar\\comflowcounter.jar");
        job.setMapperClass(FlowMapperSort.class);
        job.setReducerClass(FlowReducerSort.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/MROutput/"));
        FileOutputFormat.setOutputPath(job, new Path(("/SortOutput/")));
        /* 此处使用 MultipleOutputs 设置输出路径 保证多文件输出  mapper出也可设置分区多输出文件*/
        for (int i = 0; i < 10; i++) {
            MultipleOutputs.addNamedOutput(job, "135" + i, TextOutputFormat.class, FlowBean.class, NullWritable.class);
        }
        MultipleOutputs.addNamedOutput(job, "ERRORINPUT", TextOutputFormat.class, FlowBean.class, NullWritable.class);
        int exitCode = job.waitForCompletion(true) ? 0 : 1;
        Counters counters = job.getCounters();
        long missing = counters.findCounter(FlowMapperSort.UserDataEnum.MISSING).getValue();
        long normal = counters.findCounter(FlowMapperSort.UserDataEnum.NORMAL).getValue();
        long errorData = counters.findCounter(FlowMapperSort.UserDataEnum.ERRORData).getValue();
        long total = normal + errorData + missing;
        System.out.println("missing rate = " + 100.0 * missing / total + "%");
        System.out.println("normal rate = " + 100.0 * normal / total + "%");
        System.out.println("errorData rate = " + 100.0 * errorData / total + "%");
        return exitCode;
    }
}
