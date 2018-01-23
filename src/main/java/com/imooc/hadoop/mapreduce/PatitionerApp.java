package com.imooc.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 添加了 Patitioner功能，在43-47行、128-131行、114行
 * 输出结果有4个文件，不同的getPatition()返回值，进入不同的ReduceTask进行归并，输出结果到不同的文件中
 * 输入文本变为了 xiaomi 100 这样的形式
 * 运行：hadoop jar /home/hadoop/lib/hadoop-train-1.4.jar  com.imooc.hadoop.mapreduce.PatitionerApp /telephone.txt /output/telephone
 */
public class PatitionerApp {

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
			//words[0]为“xiaomi”，words[1]为100
			context.write(new Text(words[0]), new LongWritable(Long.parseLong(words[1])));
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

	public static class MyPartitioner extends Partitioner<Text, LongWritable>{

		@Override
		public int getPartition(Text key, LongWritable value, int numPartitions) {
			
			//不同的mapTask输出分派给不同的ReduceTask处理
			if(key.toString().equals("xiaomi")){
				return 1;
			}
			
			if(key.toString().equals("iphone")){
				return 2;
			}
			
			if(key.toString().equals("huawei")){
				return 3;
			}
			
			return 0;
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
		job.setJarByClass(PatitionerApp.class);

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
		
		// 5.1设置job的partitioner
		job.setPartitionerClass(MyPartitioner.class);
		// 5.2设置4个reduceTask，分别处理不同品牌手机数据的汇总
		job.setNumReduceTasks(4);

		// 6.设置作业处理的输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7. 提交
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion ? 0 : 1);
	}

}
