package main.java.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// 一般做法：打jar包（执行JAR file，指定Main路径：Select the class of the application entry point）
// 上传jar包到hadoop的服务器，使用hadoop命令执行：hadoop jar /上传的路径/文件名.jar，构建一个job，job提交给hadoop集群（持有jobClient）
// jobClient持有RusourceManager代理对象，通过RPC与ResourceManager通信，RM返回jobID和jar包存放路径
// jobClient将路径与jobID拼接成唯一路径，利用FileSystem 工具类写入HDFS（默认写入10份）
// 提交作业的描述信息：jobID，jar包拼接路径等信息
// ResourceManager初始化描述信息，并加入调度器
// ResourceManager计算inputSplit数量，启动多少map和reduce
// NodeManager通过心跳机制，向RM领取计算任务，并到HDFS上下载jar包，启动子线程yarnChild（map或reduce任务）
// yarnChild中map读取hdfs的数据计算后传给reduce计算，reduce将结果写回到hdfs上
public class WordCount {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		//
		//conf.setInt("mapreduce.client.submit.file.replication", "20");
		
		// 构建job对象
		Job job = Job.getInstance(conf);
		
		//notice
		job.setJarByClass(WordCount.class);// 添加本类，作为最终运行的类
		
		/**组装自定义的map和reduce*/
		//set mapper`s property
		job.setMapperClass(WCMapper.class);// 自定义map类
		job.setMapOutputKeyClass(Text.class);// map输出的key类型
		job.setMapOutputValueClass(LongWritable.class);// map输出的value类型
		FileInputFormat.setInputPaths(job, new Path("/root/words.txt"));// map输入reduce数据的hdfs路径，hadoop的path
		
		//set reducer`s property
		job.setReducerClass(WCReducer.class);// 自定义reduce类
		job.setOutputKeyClass(Text.class);// 既可以是map的输出，也可以是reduce的输出（可以只有一个map阶段，搬砖和砌墙的关系）
		job.setOutputValueClass(LongWritable.class);// 同上
		FileOutputFormat.setOutputPath(job, new Path("/root/wcout"));// reduce输出数据写入hdfs的路径
		
		//submit
		job.waitForCompletion(true);// 提交作业，true：打印进度和详情
	}

}
