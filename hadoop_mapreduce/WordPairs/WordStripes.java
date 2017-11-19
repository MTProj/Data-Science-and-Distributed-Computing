import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.*;
import java.lang.StringBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordStripes {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, myMapWritable>{

    private myMapWritable neighborsMapWritable = new myMapWritable();
    private IntWritable one = new IntWritable(1); 
    
    private Text word = new Text();
    private Text neighbor = new Text();
    
    private HashMap<String,Integer> neighborsMap = new HashMap<String,Integer>();
   
    public void map(Object key, Text value, Context context) 
    		throws IOException, InterruptedException {
   
  	StringTokenizer itr = new StringTokenizer(value.toString());
  	ArrayList<String> line = new ArrayList<String>();
  	
  	
  	
  	//Convert tokens to array list of words
  	while(itr.hasMoreTokens()){
  		line.add(itr.nextToken());
  	}
  	//Add neighbors to HashMap for each word
  	for(int i = 0; i < line.size() - 1; i++){
  		neighborsMap.clear();
  		neighborsMapWritable.clear();
  	
  		String wordString = line.get(i);
  		cleanString(wordString);
  		
  		//This is what will be written to reducer as key
  		word.set(wordString);
  		
  		if(SkipWord(toText(wordString))){
  			//Skip the word..
  		}else{
  	  		for(int j = i + 1; j < line.size(); j++){
  	  			String neighborString = line.get(j);
  	  			cleanString(neighborString);
  	  			if(SkipWord(toText(neighborString))){
  	  				//Skip the word..
  	  			}else{
  	  				if(neighborsMap.containsKey(neighborString)){
  	  					//Increment count by 1
  	  					int sum = neighborsMap.get(neighborString);
  	  					sum = sum + 1;
  	  					neighborsMap.put(neighborString, sum);
  	  				}else{
  	  					//Put neighbor in with value 1
  	  					neighborsMap.put(neighborString,1);
  	  				}
  	  				
  	  			}
  	  		}
  	  		/*
  	  		 *  Convert HashMap to MapWritable then call context.write()
  	  		 *  
  	  		 *  I am not using a MapWritable by default to there being some type of err
  	  		 *  in the MapWritable.put method. When putting, what happens is the keys actually
  	  		 *  overwrite one another and you end up with a map of exact keys. This could be fixed
  	  		 *  by writing a new MapWritable class that extends MapWritable and overriding the 
  	  		 *  put method. For now, I am going to use a more hacky method.
  	  		 */
  	  		neighborsMapWritable = toMapWritable(neighborsMap);
	  		context.write(word,neighborsMapWritable);
  		}
  	 }
    }
  }
   
  public static class StripeReducer
       extends Reducer<Text,myMapWritable,Text,myMapWritable> {
	  
	private myMapWritable mapResult = new myMapWritable();

    public void reduce(Text key,Iterable<myMapWritable> values,Context context) 
    		throws IOException, InterruptedException {
    
    /*
     *  Clear mapResult. Each time the Reduce method is called mapResult will be used
     *  and written using context.write(). It may not actually be necessary to do this 
     *  because context.write() may clear the variable. But I am not 100% sure so I will do
     *  this to be safe. - Need to look into what happens to an object when it is passed to context.write.
     */
    
    mapResult.clear();
    
    //Iterate over all maps sent to the reducer
	for(myMapWritable value: values){
		
		//Get keySet from current map 'value'
    	Set<Writable> keys = value.keySet();
    	
    	//Iterate over values in map
		for(Writable mapkey : keys){
			
			/*
			 *  Create new IntWritable - Set to value found in at the key in currentMap
			 *  If that key has already been added to mapResult , then just increment the count by 1 of that key
			 *  	else add that key to the mapResult
			 *  
			 *  It is necessary to create a new IntWritable each time since it 'seems' that the object will not simply
			 *  replace what has already been set , if using IntWritable.set(). Also, need to cast it to an IntWritable.
			 *  
			 *  When trying to just create the new IntWritable I needed to cast it to an IntWritable since it is coming from
			 *  a Writable.
			 *  
			 */
			
			//Create new IntWritable , get value from key and cast to IntWritable
			IntWritable keyValue = (IntWritable) value.get(mapkey);
			if(mapResult.containsKey(mapkey)){
				
				//Key is already in map
				//Create new IntWritable to record current value in MapResult
				//Set to new value
				IntWritable resultKeyValue = (IntWritable) mapResult.get(mapkey);
				resultKeyValue.set(resultKeyValue.get() + keyValue.get());
				
			}else{
				mapResult.put(new Text(mapkey.toString()),keyValue);
			}
			
		}
		
    }
	context.write(key,mapResult);
    }
  }
  
  /*
   * Helper Methods
   */
  public static Text incrementValueByOne(Text t){
	    Text value = new Text();
	    String data = t.toString();
		int valueInt = Integer.parseInt(data);
		valueInt = valueInt + 1;
		value.set(Integer.toString(valueInt));
		return value;
  }
  public static String cleanString(String s){
	  String s1 = s;
	  s1 = s1.replace("\"","");
	  s1 = s1.replace(",","");
	  s1 = s1.replace(".","");
	  return s1;
  }
  
  public static Boolean SkipWord(Text t){
	    if(t.find("<") != -1 || t.find(">") != -1){
			//Skip Word
	    	return true;
		}else if(t.find("https://") != -1){
			//Skip Word
			return true;
		}else if(t.find("/") != -1 || t.find("\"") != -1){
			//Skip Word
			return true;
		}else{
			return false;
		}
  }
  
  public static Text toText(String s){
	  Text t = new Text();
	  t.set(s);
	  return t;
  }
  public static IntWritable toIntWritable(int i){
	  IntWritable iw = new IntWritable();
	  iw.set(i);
	  return iw;
  }
  
  public static myMapWritable toMapWritable(HashMap<String,Integer> map){
	  
	  myMapWritable mw = new myMapWritable();
	  
	  for(String it : map.keySet()){
		  mw.put(new Text(it.toString()),new IntWritable(map.get(it)));
	  }
	  return mw;
  }
  
  public static class myMapWritable extends MapWritable{
	  @Override
	  public String toString(){
		  String result = "";
		  
		  
		  for(Writable key : this.keySet()){
			 result = result + key.toString() + "-" + this.get(key) + ",";
		  }
		  
		  result = "{" + result + "}";
		  
		  return result;
	  }
  }
  public static void main(String[] args) throws Exception {
    
	  
    Configuration conf = new Configuration();
    conf.set("mapreduce.textoutputformat.separator",",");
    Job job = Job.getInstance(conf, "Word Co-Occurances Stripes");
    job.setJarByClass(WordStripes.class);
    
    
    job.setNumReduceTasks(1);
    
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(StripeReducer.class);
    job.setReducerClass(StripeReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(myMapWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
