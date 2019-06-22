import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = value.toString();
	  String fields[] = line.split(",");
	  String outputKey = "";
	  String outputValue = "";
	  
	  ArrayList<String> weekends = new ArrayList<String>(
			  Arrays.asList("Saturday","Sunday"));
	  
	  if(!fields[3].equals("0")){	  
		  
		  if(weekends.contains(fields[5])){
			  outputKey = fields[7]+","+fields[6]+","+fields[0]+","+fields[3];
			  outputValue = "WE"+fields[4];
		  }else{
			  outputKey = fields[7]+","+fields[6]+","+fields[0]+","+fields[3];
			  outputValue = "WD"+fields[4];
		  }
		  	  
		  context.write(new Text(outputKey), new Text(outputValue));
	  }

  }
}
