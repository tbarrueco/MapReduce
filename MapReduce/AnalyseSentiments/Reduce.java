
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, FloatWritable> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
    	//definition de variables 	
    	float valsentiment = 0;
        int pos=0;
        int neg=0;
        int totalpos=0;
        int totalneg=0;
       
      //on commence a parcourir la liste de valeurs
        while (values.hasNext()) {             	
      	String posneg=values.next().toString();  
      	String[] parts = posneg.split("-");
      //on ajoute tous le numeros de mots positives et negatives
      	if (posneg.length()>0){     		
      	    pos=Integer.parseInt(parts[0].trim());
          	neg=Integer.parseInt(parts[1].trim());
          	totalpos+=pos;
          	totalneg+=neg;
      	}
      	}
      //on va ajouter la formule pour le calcule des analyse des sentiments
        int dividendo=(totalpos-totalneg);
        int divisor=(totalpos+totalneg);
        valsentiment=(float)dividendo/divisor;
       //on ecrit la cle et le valeur 
        output.collect(key, new FloatWritable(valsentiment));
    }
  }

