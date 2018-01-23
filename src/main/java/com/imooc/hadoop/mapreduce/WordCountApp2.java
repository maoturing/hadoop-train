package com.imooc.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 添加了判断输出文件是否存在的逻辑，在81行
 */
public class WordCountApp2 {

	/**
	 * Mapper：可以重写三个方法map,setup,cleanup.其中map中实现业务逻辑
	 * 读取输入的文件 Text:类似字符串 Mapper:模板设计模式，分为setUp map
	 * cleanUp三个方法，由run方法调用，我们一般只需要重写map方法 区中setUp和CleanUp只在开始和结束时被调用一次
	 * Mapper<LongWritable,Text,Text,LongWritable>：LongWritable1是偏移量，第一行是从0开始，第二行就是从0+LongWritable开始
	 * 		Text1是每行的字符串，Text2是映射的key，LongWritable就是映射的value。
	 * 
	 * 运行：hadoop jar lib/hadoop-train-1.1.jar  com.imooc.hadoop.mapreduce.WordCountApp /hello.txt /output/wc
	 * 结果：hadoop fs -text /output/wc/part-r-00000
	 *
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		LongWritable one = new LongWritable(1);

		/**
		 * key: text：文本 context:映射结果
		 */
		@Override
		protected void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
			//对接受到的每一行数据进行分割
			String line = text.toString();
			String[] words = line.split(" ");
			
			for (String word : words) {
				//通过上下文将map的处理结果输出
				context.write(new Text(word), one);
			}
		}

	}

	/**
	 * 归并操作 key:关键字       values：很多个1    context：归并结果
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
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 * 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		//0.清理已存在的输出目录
		Path outputPath = new Path(args[1]);
		FileSystem filesystem = FileSystem.get(conf);
		if(filesystem.exists(outputPath)){
			filesystem.delete(outputPath, true);
			System.out.println("output file exists, but is has deleted ");
		}
		
		
		// 1.创建Configurationg和Job
		Job job = Job.getInstance(conf, "word-count");

		// 2.设置job的处理类
		job.setJarByClass(WordCountApp2.class);

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

}
