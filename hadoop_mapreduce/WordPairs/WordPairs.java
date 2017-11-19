import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordPairs {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text neighbor = new Text();
    private Text word_neighbor = new Text();
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    

   // Tokenize the line - (Split each word into a token seperated by spaces)
  	StringTokenizer itr = new StringTokenizer(value.toString());
  	ArrayList<String> line = new ArrayList<String>();
  	while(itr.hasMoreTokens()){
  		line.add(itr.nextToken());
  	}
  	//Perform Map operation
  	for(int i = 0; i < line.size() - 1; i++){
  		String s = line.get(i);
  		s = s.replace("\"","");
  		s = s.replace(",","");
  		s = s.replace(".","");
  		word.set(s);
  		if(word.find("<") != -1 || word.find(">") != -1){
  			//Skip Word
  		}else if(word.find("https://") != -1){
  			//Skip Word
  		}else if(word.find("/") != -1 || word.find("\\") != -1){
  			//Skip Word
  		}else{
  			/*
  			 *	Get the pairs
  			 */
  			for(int j= i + 1; j < line.size(); j++){
  				String s2 = line.get(j);
  				s2 = s2.replace("\"","");
  				s2 = s2.replace(",","");
  				s2 = s2.replace(".","");
  				neighbor.set(s2);		
  					if(neighbor.find("<") != -1 || neighbor.find(">") != -1){
  						//Skip Word
  					}else if(neighbor.find("https://") != -1){
  						//Skip Word
  					}else if(neighbor.find("/") != -1 || neighbor.find("\"") != -1){
  						//Skip Word
  					}else{
  						/*
  					 	*  Emit Pair
  					 	*/
  						String phrase = "< " + word.toString() + "," + neighbor.toString() + " >";
  						//System.out.println(phrase);
  						word_neighbor.set(phrase);
  						context.write(word_neighbor,one);
  					}
  					
  				}
  			}
  		}
  	
      }
    }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.textoutputformat.separator",",");
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordPairs.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
