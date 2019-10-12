/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/7
 * Time: 23:28
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package com.flow.inverse;

import com.flow.hdfsutil.HdfsUtil;
import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class InverseIndexStepTwo extends Configured implements Tool {

    private static class StepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //输入数据 hello---->filename   count
            String line = value.toString();
            String[] fields = StringUtils.split(line, "\t");
            String[] keyWord = StringUtils.split(fields[0], "---->");//hello filename
            String filename = keyWord[1];
            long count = Long.parseLong(fields[1]);//count
            // <hello,filename---->count>
            context.write(new Text(keyWord[0] + "\t"), new Text("\t" + filename + "---->" + count));
        }
    }

    private static class StepTwoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //得到的数据<hello,{a.txt--->3,b.txt--->1}>
            String indexValue = "";
            for (Text value : values) {
                indexValue += value.toString() + "\t";
            }
            context.write(key, new Text(indexValue));
        }
    }

    private static Log logger = LogFactory.getLog(InverseIndexStepTwo.class);

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        HdfsUtil hdfsUtil = new HdfsUtil();
        String filename = "/StepTwoOutput/";
        if (hdfsUtil.rmdir(filename)) {
            logger.info(filename + " 删除成功");
        }

        job.setJarByClass(InverseIndexStepTwo.class);
        job.setJar("out\\artifacts\\comflowcounter_jar\\comflowcounter.jar");

        job.setMapperClass(StepTwoMapper.class);
        job.setReducerClass(StepTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("/StepOneOutput/"));
        FileOutputFormat.setOutputPath(job, new Path("/StepTwoOutput/"));
        int resStepTwo = job.waitForCompletion(true) ? 0 : 1;
        if (resStepTwo == 0) {
            hdfsUtil.listFiles(filename, true);
            hdfsUtil.printFileContent(filename + "/part-r-00000", 10);
        }

        return resStepTwo;
    }
}
