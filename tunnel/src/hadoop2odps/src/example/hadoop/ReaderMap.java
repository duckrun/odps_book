package example.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.DataTunnel;
import com.aliyun.odps.tunnel.UploadSession;

import example.util.DesUtils;

public class ReaderMap {
 
  public static class LoadMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, IntWritable>{
    
    private RecordWriter recordWriter;
    private TableSchema tableSchema;
    private Record record;  
    
    public void configure(JobConf conf) {
    
      try {
        DesUtils util = new DesUtils();
        
        //init & get upload session
        Account account = new AliyunAccount(
            util.decrypt(conf.get("odps.accessid")), 
            util.decrypt(conf.get("odps.accesskey")));
        Odps odps = new Odps(account);
        odps.setDefaultProject(conf.get("odps.project"));
        odps.setEndpoint(conf.get("odps.endpoint")); 
        
        DataTunnel tunnel = new DataTunnel(odps);
        UploadSession upload;
        if(conf.get("odps.partition") != null) {
          PartitionSpec spec = new PartitionSpec(conf.get("odps.partition"));
          upload= tunnel.getUploadSession(
              conf.get("odps.project"), 
              conf.get("odps.table"),
              spec,
              conf.get("tunnel.uploadid"));
        }
        else
        {
          upload = tunnel.getUploadSession(
              conf.get("odps.project"), 
              conf.get("odps.table"),
              conf.get("tunnel.uploadid"));
        }
        
        recordWriter = upload.openRecordWriter(conf.getLong("mapred.task.partition", -1L), true);
        tableSchema = upload.getSchema();
        record = upload.newRecord();
        
      }catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void map(LongWritable key, 
                    Text value, 
                    OutputCollector<Text, IntWritable> output, 
                    Reporter reporter)
                    throws IOException{
      if (tableSchema.getColumn(0).getType() == OdpsType.STRING) {
        record.setString(0, value.toString());
        recordWriter.write(record);
      }
    }
    
    public void close() throws IOException{
      recordWriter.close();
    }
  }
  
}