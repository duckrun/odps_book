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

public class GetFinalTopk2 {

  public static class TopkMapper extends MapperBase {
    private Record word;
    private Record cnt;

    @Override
    public void setup(TaskContext context) throws IOException {
      cnt = context.createMapOutputKeyRecord();
      word = context.createMapOutputValueRecord();      
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
      word.set(0, record.get(0));
      cnt.set(0, 0 - record.getBigint(1));  // -cnt
      context.write(cnt, word);      
    }
  }
  
  public static class TopkReducer extends ReducerBase {
    private Record result = null;        
    int k = 0;
    int count = 0;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
      k = Integer.parseInt((context.getJobConf().get("k")));
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {  
      while (count++ < k && values.hasNext()) {
        Record val = values.next();
        result.set(0, val.get(0));
        result.set(1, 0 - key.getBigint(0));
        context.write(result); 
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: GetFinalTopk2 <in_table> <out_table> <k>");
      System.exit(2);
    }

    JobConf job = new JobConf();

    job.setMapperClass(TopkMapper.class);
    job.setReducerClass(TopkReducer.class);
    job.setNumReduceTasks(1);

    job.setMapOutputKeySchema(SchemaUtils.fromString("count:bigint"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("word:string"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
    
    job.set("k", args[2]);

    JobClient.runJob(job);
  }

}
