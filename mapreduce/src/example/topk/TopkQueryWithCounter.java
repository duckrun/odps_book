package example.topk;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

import example.util.MinHeap;
import example.util.Pair;

public class TopkQueryWithCounter {
  enum DataCounter {
    STOP_WORDS,
    DIRTY_WORDS
  }
  
  enum TaskCounter {
    MAP_TASKS, 
    COMBINE_TASKS,
    REDUCE_TASKS
  }

  public static class TopkMapper extends MapperBase {
    private Record word;
    private Record one;
    private Pattern pattern;
    // store stopwords
    Map stopMap = new HashMap<String,Integer>();   
    
    // data counters
    Counter stopCounter;
    Counter dirtyCounter;    

    @Override
    public void setup(TaskContext context) throws IOException {
      word = context.createMapOutputKeyRecord();
      one = context.createMapOutputValueRecord();
      one.setBigint(0, 1L);
      pattern = Pattern.compile("\\s+");
      
      // read resource
      BufferedInputStream  stream= null;
      DataInputStream dstream = null;
      
      String file = context.getJobConf().get("stopwords");
      stream = context.readResourceFileAsStream(file);
      dstream = new DataInputStream(stream);
      
      while(dstream.available() != 0) {
        stopMap.put(dstream.readLine(), 1);
      }
      
      stream.close();
      dstream.close();
      
      // data counters
      stopCounter = context.getCounter(DataCounter.STOP_WORDS);
      // if a word not start with letter, consider it dirty 
      dirtyCounter = context.getCounter(DataCounter.DIRTY_WORDS);
      
      // task counter
      Counter taskCounter = context.getCounter(TaskCounter.MAP_TASKS);
      taskCounter.increment(1);
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
      for (int i = 0; i < record.getColumnCount(); i++) {
        String[] words = pattern.split(record.get(i).toString());
        for (String w : words) {
          if(!stopMap.containsKey(w) && Character.isLetter(0)) {            
            word.setString(0, w); 
            context.write(word, one);
          }
          else if(stopMap.containsKey(w)){
            stopCounter.increment(1);
          }
          else {
            dirtyCounter.increment(1);
          }
        }
      }
    }
  }

  /**
   * A combiner class that combines map output by sum them.
   **/
  public static class TopkCombiner extends ReducerBase {
    private Record count;

    @Override
    public void setup(TaskContext context) throws IOException {
      count = context.createMapOutputValueRecord();
      // task counters
      Counter taskCounter = context.getCounter(TaskCounter.COMBINE_TASKS);
      taskCounter.increment(1);      
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      long c = 0;
      while (values.hasNext()) {
        Record val = values.next();
        c += (Long) val.get(0);
      }
      count.setBigint(0, c);
      context.write(key, count);
    }
  }

  /**
   * A reducer class that emits the topk results
   **/
  public static class TopkReducer extends ReducerBase {
    private Record result = null;    
    
    List<Pair<String, Long>> array = new ArrayList<Pair<String, Long>>();
    MinHeap heap;
    int k = 0;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
      k = Integer.parseInt((context.getJobConf().get("k")));
      heap = new MinHeap(array);
      
      // add counters
      Counter taskCounter = context.getCounter(TaskCounter.REDUCE_TASKS);
      taskCounter.increment(1);
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      long count = 0;
      while (values.hasNext()) {
        Record val = values.next();
        count += (Long) val.get(0);
      }      
      Pair<String, Long> pair= new Pair(key.get(0), count);
      heap.getTopK(pair, k);
    }

    public void cleanup(TaskContext context) throws IOException {
      for(Pair<String, Long> p: heap.get()) {
        result.set(0, p.getLeft());
        result.setBigint(1, p.getRight());
        context.write(result);        
      }      
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: TopkQuery <in_table> <out_table> <k> <resource_of_stopwords>");
      System.exit(2);
    }

    JobConf job = new JobConf();

    job.setMapperClass(TopkMapper.class);
    job.setCombinerClass(TopkCombiner.class);
    job.setReducerClass(TopkReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
    
    job.set("k", args[2]);
    job.set("stopwords", args[3]);
    
    JobClient.runJob(job);
    
    // the following is unnecessary, the MR system will output user defined counters

    //RunningJob rjob = JobClient.runJob(job);
    
    // get counters
    /*Counters counters = rjob.getCounters();
    long dirtyCnt = counters.findCounter(DataCounter.DIRTY_WORDS).getValue();
    long stopCnt = counters.findCounter(DataCounter.STOP_WORDS).getValue();
    long mapCnt = counters.findCounter(TaskCounter.MAP_TASKS).getValue();
    long combineCnt = counters.findCounter(TaskCounter.COMBINE_TASKS).getValue();
    long reduceCnt = counters.findCounter(TaskCounter.REDUCE_TASKS).getValue();
    
    System.out.println("JOB Finished.\n" + 
        "dirtyCnt:" + dirtyCnt +
        ",stopCnt:" + stopCnt  +
        ",mapCnt:"  + mapCnt   +
        ",combineCnt:"  + combineCnt  +
        ",reduceCnt:"   + reduceCnt
        );*/
    
    
  }

}
