import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class CommonReducer extends Reducer<Text, Text, NullWritable, Text> {
  
  HashMap<Integer, String> promo = new HashMap<Integer, String>();
  @Override
  public void setup(Context context)throws IOException,InterruptedException,FileNotFoundException{
	  String filename;
	  BufferedReader reader;
	  String line;
	  Integer hashkey;
	  String hashval;
	  int partitionIndex;
	  
	  try{
		  Path promo_files[] = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		  for(Path file: promo_files ){
			  filename = file.getName().trim();
			  if(filename.contains("part")){
				  reader = new BufferedReader(new FileReader(file.toString()));
				  line = reader.readLine();
				  while(line != null){
					  line = reader.readLine();
					  partitionIndex = line.indexOf(",");
					  hashkey = Integer.parseInt(line.substring(0, partitionIndex));
					  hashval = line.substring(partitionIndex+1, line.length());
					  promo.put(hashkey, hashval);
				  }
				  reader.close();
			  } 
		  }
	  }catch(Exception e){
		  e.printStackTrace();
	  }
  }
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  Float WD_sales_sum = 0.0f;
	  Float WE_sales_sum = 0.0f;
	  String outputVal;
	  Integer joinkey;
	  String outputKeyfields[];
	  
	  for(Text inp : values){	  
		  String input = inp.toString();	  
		  String week_ind = input.substring(0,2);
		  Float sales =  Float.parseFloat(input.substring(2,input.length()));
		  
		  if(week_ind.equals("WD")){			  
			  WD_sales_sum += sales;	  
		  }else if(week_ind.equals("WE")){			  
			  WE_sales_sum += sales;
		  }		  
	  }
	  
	  outputKeyfields = key.toString().split(",");
	  joinkey = Integer.parseInt(outputKeyfields[3]);
	  
	  outputVal = key+","+promo.get(joinkey)+","+WD_sales_sum+","+WE_sales_sum;  
	  context.write(NullWritable.get(),new Text(outputVal));
  }
}