/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/7
 * Time: 22:39
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package com.flow.inverse;

import com.flow.hdfsutil.HdfsUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/*
 *  倒排索引job
 *  1. 完成每个文件的独立统计
 *   hello ---> a.txt 5
 *
 * */
public class InverseIndexStepOne extends Configured implements Tool {

    private static final Log logger = LogFactory.getLog(InverseIndexStepOne.class);

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        HdfsUtil hdfsUtil = new HdfsUtil();
        String filename = "/StepOneOutput/";
        if (hdfsUtil.rmdir(filename)) {
            logger.info(filename + " 删除成功");
        }
        job.setJarByClass(InverseIndexStepOne.class);
        job.setJar("out\\artifacts\\comflowcounter_jar\\comflowcounter.jar");
        job.setMapperClass(StepOneMapper.class);
        job.setReducerClass(SteopReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/StepOneInput"));
        FileOutputFormat.setOutputPath(job, new Path("/StepOneOutput/"));

        int resStepOne = job.waitForCompletion(true) ? 0 : 1;
        if (resStepOne == 0) {
            hdfsUtil.listFiles(filename, true);
            hdfsUtil.printFileContent(filename + "/part-r-00000");
            hdfsUtil.rm(filename + "_SUCCESS");
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class StepOneMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拿到一行数据
            String line = value.toString();
            String[] fields = StringUtils.split(line, ' ');
            //从切片中获取文件名
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            Path locationInfo = inputSplit.getPath();
            String filename = locationInfo.getName();
            for (String field : fields) {
                //封装kv，每个hello 封装值为1，然后利用reduce累加
                context.write(new Text(field + "---->" + filename), new LongWritable(1));
            }
        }
    }

    private static class SteopReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long counter = 0;
            for (LongWritable longWritable : values) {
                counter += longWritable.get();
            }
            context.write(key, new LongWritable(counter));
        }
    }

}
