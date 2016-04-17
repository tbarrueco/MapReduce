// TERESA BARRUECO/////
// Analyse de sentiments de Shakespeare///
// Avril 2016///

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AnalyseDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// configuration du job mapreduce
		JobConf conf = new JobConf(getConf(), AnalyseDriver.class);
		conf.setJobName("analysesent");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FloatWritable.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
        conf.setMapOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
				
		
		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			// on ajoute le fichier des mots positives au cache
			if ("-pos".equals(args[i])) {
			   conf.setBoolean("analysesent.pos.patterns", true);
				i += 1;
				DistributedCache.addCacheFile(new Path(args[i]).toUri(),conf);
				// on ajoute le fichier des mots negatives au cache	
			}else if ("-neg".equals(args[i])) {
				conf.setBoolean("analysesent.neg.patterns", true);
				i += 1;
				DistributedCache.addCacheFile(new Path(args[i]).toUri(),conf);
			}else {
				// le reste d'arguments avec le fichier d'entree et sortie
				other_args.add(args[i]);
			}
		}

		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AnalyseDriver(), args);
		System.exit(res);
	}
}


