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

public class GetFinalTopk {

  public static class TopkMapper extends MapperBase {
    private Record word;
    private Record cnt;

    @Override
    public void setup(TaskContext context) throws IOException {
      word = context.createMapOutputKeyRecord();
      cnt = context.createMapOutputValueRecord();      
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
      word.set(0, record.get(0));
      cnt.set(0, record.get(1));
      context.write(word, cnt);      
    }
  }
  
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
      Pair<String, Long> pair= new Pair(key.get(0), values.next().get(0));
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
    if (args.length != 3) {
      System.err.println("Usage: GetFinalTopk <in_table> <out_table> <k>");
      System.exit(2);
    }

    JobConf job = new JobConf();

    job.setMapperClass(TopkMapper.class);
    job.setReducerClass(TopkReducer.class);
    job.setNumReduceTasks(1);

    job.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
    
    job.set("k", args[2]);

    JobClient.runJob(job);
  }

}
