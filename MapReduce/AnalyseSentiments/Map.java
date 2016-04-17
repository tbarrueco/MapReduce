

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;


public class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    // on declare les variables pour les mots positives et negatives
    private Text word = new Text();
     
	private Set<String> goodWords = new HashSet<String>();
	private Set<String> badWords = new HashSet<String>();
	
    private long numRecords = 0;
    private String inputFile;

    public void configure(JobConf job) {
      inputFile = job.get("map.input.file");
      // On va prendre les paths des fichiers avec les mots positives et negatives
      //et on va passer le path a une function 
      if (job.getBoolean("analysesent.pos.patterns", false)&&job.getBoolean("analysesent.neg.patterns", false)) {
    	  Path[] patternsFiles = new Path[0];
        try {
          patternsFiles = DistributedCache.getLocalCacheFiles(job);
        } catch (IOException ioe) {
          System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
        }
        if (patternsFiles.length>0){
        	parsePositive(patternsFiles[0]);
        	parseNegative(patternsFiles[1]);
        }
      }else{
    	  System.out.println("pos patterns false");
      }  
      
    }  
// function pour ajouter tous les mots positives a la variable goodWords
    private void parsePositive(Path patternsFile) {
		try {
			BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
			String goodWord;
			while ((goodWord = fis.readLine()) != null) {
				goodWords.add(goodWord);
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception parsing cached file '"
					+ goodWords + "' : " + StringUtils.stringifyException(ioe));
		}
	}
  
 // function pour ajouter tous les mots negatives a la variable badWords
	private void parseNegative(Path patternsFile) {
		try {
			BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));String badWord;
			while ((badWord = fis.readLine()) != null) {
				badWords.add(badWord);
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing cached file '"
					+ badWords + "' : " + StringUtils.stringifyException(ioe));
		}
	}
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      //on enleve les signes de punctuation
      String line= value.toString().replaceAll("\\p{Punct}|\\d", "").toLowerCase();
            
      int poswords=0;
      int negwords=0;
      // On commence a lire le fichiera qu'on veut analyser
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        //on verifie que le mot soit dans la liste de mots positives
        //si le mot est dans la liste on ajoute +1
        if (goodWords.contains(word.toString())) {
         		poswords+=1;
        }
      //on verifie que le mot soit dans la liste de mots negatives
      //si le mot est dans la liste on ajoute +1
        
        if (badWords.contains(word.toString())) {
         		negwords+=1;
        }
        //on construit comme value une string avec le numero de mots positives - mots negatives
    	String posneg = Integer.toString(poswords)+ "-" +Integer.toString(negwords);
 		// on ajoute le nom du fichier comme cle
    	String filename= inputFile.substring(inputFile.lastIndexOf('/')+1);
    	output.collect(new Text(filename),new Text(posneg));
    	
      }

      if ((++numRecords % 100) == 0) {
        reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
      }
    }
  }




