

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;
import java.io.IOException;


public class SAgeReduce extends Reducer<Text, DoubleWritable, Text, Text>
{
public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
	{
		//Definition des variables
		double salmin=0;
		double salmax=0;
		double salact=0;
		Iterator<DoubleWritable> i=values.iterator();
		int count=0;
		//On va parcourir tous les salaires par age
		while(i.hasNext()){
			salact= i.next().get();
			//on ajoute +1 au numero des personnes de cet age
			count+=1;
			//si le saire est superior, on le garde comme le salaire maximum
			if (salact>salmax){
				salmax=salact;
			}
			//sinon on le chosis comme le minimum s'il est 0 ou inferieur 
			else if((salact<salmin) | (salmin==0)){
				salmin=salact;
			}
		}	
		//on ajoute la cle et le valeur
		context.write(key, new Text("Nombre de personnes: " + count + "\t Salaire maximum: " + salmax + "\t Salaire minimum: " + salmin));
}
}
