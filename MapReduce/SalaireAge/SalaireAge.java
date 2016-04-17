
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;


public class SalaireAge
{
	public static void main(String[] args) throws Exception
	{
		// Creation object de configuration Hadoop.
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "Calcule salaire par age");

		// Definition des classes driver, map et reduce.
		job.setJarByClass(SalaireAge.class);
		job.setMapperClass(SAgeMap.class);
		job.setReducerClass(SAgeReduce.class);

		// Definition cles valeurs
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if(job.waitForCompletion(true))
			System.exit(0);
		System.exit(-1);
	}
}
