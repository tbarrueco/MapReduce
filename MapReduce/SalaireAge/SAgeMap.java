import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;



public class SAgeMap extends Mapper<Object, Text, Text, DoubleWritable>
{
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{  
		double salaire;
		//On separe les valeurs de chaque ligne separes par une virgule
		String[] tokens = value.toString().split(",");
		
		try{
		String age = tokens[1].trim();
		salaire = Double.parseDouble(tokens[4].trim());
		//On ecrit la comme cle l'age et comme valeur le salaire
		context.write(new Text(age),new DoubleWritable(salaire));
		}
		catch(NumberFormatException e){
			System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(e));
		}
	}
	 	

}
