package com.imooc.hadoop.project;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;

/**
 * 使用mapreduce统计访问日志中个浏览器的出现次数
 * 
 * @author mao
 *
 */
public class LogApp {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		LongWritable one = new LongWritable(1);
		UserAgentParser userAgentParser;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			userAgentParser = new UserAgentParser();
		}

		@Override
		protected void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
			// 对接受到的每一行数据进行分割
			String line = text.toString();

			String source = line.substring(getCharPosition(line, "\"", 7)) + 1;
			UserAgent agent = userAgentParser.parse(source);
			String browser = agent.getBrowser();

			context.write(new Text(browser), one);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			userAgentParser = null;
		}

	}

	/**
	 * 归并操作 key:关键字 values：很多个1 context：归并结果
	 */

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));
		}

	}

	/**
	 * 定义Driver：封装了MapReduce作业的所有信息
	 * 
	 * 分为7步：
	 * 
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		// 0.清理已存在的输出目录
		Path outputPath = new Path(args[1]);
		FileSystem filesystem = FileSystem.get(conf);
		if (filesystem.exists(outputPath)) {
			filesystem.delete(outputPath, true);
			System.out.println("output file exists, but is has deleted ");
		}

		// 1.创建Configurationg和Job
		Job job = Job.getInstance(conf, "word-count");

		// 2.设置job的处理类
		job.setJarByClass(LogApp.class);

		// 3.设置作业处理的输入路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// 4.设置map相关的参数 3个
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// 5.设置Reducer相关的参数 3个
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// 6.设置作业处理的输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7. 提交
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion ? 0 : 1);
	}

	/**
	 * 获取字符串中指定字符的索引位置
	 */
	public static int getCharPosition(String str, String operator, int index) {
		Matcher slashMatcher = Pattern.compile(operator).matcher(str);
		int mIdx = 0;

		while (slashMatcher.find()) {
			mIdx++;
			if (mIdx == index) {
				break;
			}
		}
		return slashMatcher.start();
	}

}
