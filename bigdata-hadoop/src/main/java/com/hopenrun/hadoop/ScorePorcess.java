package com.hopenrun.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 计算学生平均成绩
 * 
 * @author LS
 *
 */
public class ScorePorcess
{
    // Map处理
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
    {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
	    // 将输入的纯文本文件的数据转化成String
	    String line = value.toString();
	    // 输出读入的内容
	    System.out.println(line);
	    // 将输入的数据首先按行进行分割
	    StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
	    // 分别对每一行进行处理
	    while (tokenizerArticle.hasMoreTokens())
	    {
		// 每行按空格划分
		StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
		// 学生姓名部分
		String strName = tokenizerLine.nextToken();
		// 成绩部分
		String strScore = tokenizerLine.nextToken();
		// name of student
		Text name = new Text(strName);
		// score of student
		int scoreInt = Integer.parseInt(strScore);
		// 输出姓名和成绩
		context.write(name, new IntWritable(scoreInt));
	    }

	}
    }

    // Reduce处理
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException
	{
	    int sum = 0;
	    int count = 0;
	    Iterator<IntWritable> iterator = values.iterator();
	    while (iterator.hasNext())
	    {
		// 计算总分
		sum += iterator.next().get();
		// 统计总的科目数
		count++;
	    }
	    // 计算平均成绩
	    int average = (int) sum / count;
	    context.write(key, new IntWritable(average));
	}
    }

    // 本地测试
    public int run(String[] args) throws Exception
    {
	@SuppressWarnings("deprecation")
	Job job = new Job(getConf());
	job.setJarByClass(ScorePorcess.class);
	job.setJobName("ScorePorcess");

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);

	job.setMapperClass(Map.class);
	job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);

	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	boolean success = job.waitForCompletion(true);
	return success ? 0 : 1;
    }

    private Configuration getConf()
    {
	Configuration conf = new Configuration();
	conf.addResource("configuration-default.xml");
	conf.get("hadoop.tmp.dir");
	conf.get("io.file.buffer.size");
	conf.get("height");
	return conf;
    }

    public static void main(String[] args) throws Exception
    {
	int ret = ToolRunner.run((Tool) new ScorePorcess(), args);
	System.exit(ret);
    }

}
