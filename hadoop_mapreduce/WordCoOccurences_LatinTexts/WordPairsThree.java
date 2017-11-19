import java.io.IOException;
import java.io.FileReader;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.*;
import java.lang.StringBuilder;
import java.io.StringReader;
import java.io.BufferedReader;
import java.text.NumberFormat;
import java.text.DecimalFormat;

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

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.io.BufferedReader;
public class WordPairsThree {
	
	static BufferedReader lemmaFile;
	static String lastLocation;

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, Text>{
	
	  
    private Text location = new Text();
    private Text word = new Text();
    
    
    public void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException {
    
    	String line = value.toString();
    	String[] r = line.split(">");
    	
    	if(r.length == 2){
    		lastLocation = r[0];
    		String loc = r[0] + ">";
    		location.set(loc);
    		StringTokenizer itr = new StringTokenizer(r[1]);
    		ArrayList<String> lineArray = new ArrayList<String>();
    		while(itr.hasMoreTokens()){
    		/*
    		 * Put into arrayList for easier parsing
    		 */
    		  lineArray.add(itr.nextToken());
    		}
    		for(int i = 0; i < lineArray.size() -2; i++){
    			String s1 = lineArray.get(i);
    			String s2 = lineArray.get(i+1);
    			for(int j = i + 2; j < lineArray.size(); j++){
    				String s3 = lineArray.get(j);
    				String tuple = "{ " + s1 + "," + s2 + "," + s3 + "}";
    				word.set(tuple);
    				context.write(word,location);
    			}
    		}
    	}else{
    		/*
    		 * Skip Line. The line is not seperated by a tab with the location.
    		 */
    		location.set(lastLocation);
    		StringTokenizer itr = new StringTokenizer(r[0]);
    		ArrayList<String> lineArray = new ArrayList<String>();
    		while(itr.hasMoreTokens()){
    		/*
    		 * Put into arrayList for easier parsing
    		 */
    			lineArray.add(itr.nextToken());
    		}for(int i = 0; i < lineArray.size() -2; i++){
    			String s1 = lineArray.get(i);
    			String s2 = lineArray.get(i+1);
    			for(int j = i + 2; j < lineArray.size(); j++){
    				String s3 = lineArray.get(j);
    				String tuple = "{ " + s1 + "," + s2 + "," + s3 + "}";
    				word.set(tuple);
    				context.write(word,location);
    			}
    		}
    	}

    }   
  }
  public static class LemmaReducer
       extends Reducer<Text,Text,Text,Text> {
	 
	  
	
	private ArrayList locations = new ArrayList();
	
	private Text locationWritable = new Text();
	private Text wordWritable = new Text();

    public void reduce(Text key,Iterable<Text> values,Context context) 
    		throws IOException, InterruptedException {	

    	BufferedReader br = new BufferedReader(lemmaFile);
    	ArrayList<String> lemmaList = new ArrayList<String>(); 
    	/*
    	 * Add lemmas to arraylist
    	 */
    	try{
    		int i = 0;
    		while(br.readLine()!=null){
    			String line = br.readLine();
    			if(line != null){
    				lemmaList.add(line);
    			}
    			   			
    			//System.out.println(lemmaList.get(i));
    			i++;
    		}
    	}catch(NullPointerException e){
    		br.close();
    	}catch(IOException e1){
    		br.close();
    	}
    	/*
    	 * Normalize Word
    	 * Replace j with i , v with u
    	 * 
    	 */
    	String keyString = key.toString();
    	//keyString = keyString.replaceAll("[^a-zA-z]+","");
    	keyString = keyString.replaceAll("j", "i");
    	keyString = keyString.replaceAll("v", "u");
    	/*
    	 * Add all values from mappers passed in to an ArrayList
    	 * and build locationWritable
    	 */
    	ArrayList valuesList = new ArrayList();
    	String locString = "";
    	for(Text val : values){
    		valuesList.add(val.toString());
    		locString = locString + val.toString();    		
    	}
    	locationWritable.set(locString.toString());
    	/*
    	 * Check if word matches a word on the lemmatizer.
    	 * 	If it does set the lemma as the word and pass the location and lemma
    	 * 	else
    	 * 	set the normalized word and pass that word and the locations 
    	 * 
    	 */
    		boolean found = false;
    		int d = 0;
    		int size = lemmaList.size();
    		while(d < lemmaList.size() - 1){
        		String line = lemmaList.get(d);
        		//line = removeTrailingCommas(line);
        		String split[] = line.split(",");
        		for(int i = 0;i < split.length-1;i++){
        			if(keyString == split[i] && split[i] != null){
        				//Found a match. Add Lemma line
        				//System.out.println("Found a match on  " + keyString + " with lemma-" + line);
        				wordWritable.set(line);
        				found = true;
        			}
        		}
        		d++;
        	}
    		if(found == false){
    			wordWritable.set(keyString);
    		}
    	context.write(wordWritable,locationWritable);
    	
    }
  }
    			
  /*
   * Helper Methods
   */
  
  public static HashMap addLemmaToMap(HashMap<String, ArrayList> m,ArrayList<String>valuesList,String lemma){
	  	/*
		 * Add Lemma to Map
		 */
		if(m.containsKey(lemma)){
			/*
			 * Add Value to ArrayList if it is not already there
			 */
			for(int d = 0;d < valuesList.size()-1;d++){
				if(m.get(lemma).contains(valuesList.get(d))){
					//Skip that location it is already in the list
				}else{
					m.get(lemma).add(valuesList.get(d));
				}
			}
		}else{
			/*
			 * Add Lemma and it's locations to map
			 */
			m.put(lemma,valuesList);
		}
		
		HashMap<String,ArrayList> newMap = m;
		return newMap;
  }
  
  public static String cleanString(String s){
	  String s1 = s;
	  s1 = s1.replace("\"","");
	  s1 = s1.replace(",","");
	  s1 = s1.replace(".","");
	  return s1;
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
/*
  public static String removeTrailingCommas(String s){
	      String LemmaString = "";
		  String line = s;
		  String[] r = line.split(",");
		  String newLine = "";
		  for(int i = 0; i < r.length; i++){
			  if(r[i] == ","){
				  //Skip
			  }else{
				  LemmaString = LemmaString + r[i] + ",";
				  for(int d = 0; d < LemmaString.length()-2; d++){
					  newLine = newLine + LemmaString[d];
				  }
			  }
		  }
	  return newLine;
  }
  */
  public static void main(String[] args) throws Exception {
    
	  
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "WordCount on Classical Latin Text");
    job.setJarByClass(WordPairsThree.class);
    
    
    job.setNumReduceTasks(1);
    
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(LemmaReducer.class);
    job.setReducerClass(LemmaReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    //Read In Lemmatizer
    lemmaFile = new BufferedReader(new FileReader("new_lemmatizer.csv"));
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    long startTime = System.currentTimeMillis();  
    job.waitForCompletion(true);
    long endTime = System.currentTimeMillis();
    
    NumberFormat formatter = new DecimalFormat("#0.00");
    String duration = formatter.format((endTime - startTime));
    System.out.println("##################################################");
    System.out.println("# Map Reduce Job Time: " + duration + "ms");
    System.out.println("# Number of Reducers: " + 10);
    System.out.println("##################################################");
    
    System.exit(1);
    
  }
}
