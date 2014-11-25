package example.topk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

import example.util.MinHeap;
import example.util.Pair;

public class TopkQuery {

  public static class TopkMapper extends MapperBase {
    private Record word;
    private Record one;
    private Pattern pattern;

    @Override
    public void setup(TaskContext context) throws IOException {
      word = context.createMapOutputKeyRecord();
      one = context.createMapOutputValueRecord();
      one.setBigint(0, 1L);
      pattern = Pattern.compile("\\s+");
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
      for (int i = 0; i < record.getColumnCount(); i++) {
        String[] words = pattern.split(record.get(i).toString());
        for (String w : words) {
          word.setString(0, w); 
          context.write(word, one);
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
      heap.getTopK(pair,k);
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
    if (args.length != 3) {
      System.err.println("Usage: TopkQuery <in_table> <out_table> <k>");
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

    JobClient.runJob(job);
  }

}
