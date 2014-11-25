package example.hadoop;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.DataTunnel;
import com.aliyun.odps.tunnel.UploadSession;

import example.hadoop.ReaderMap.LoadMapper;
import example.util.DesUtils;
import org.apache.commons.codec.binary.Base64;

public class Hdfs2ODPS extends Configured implements Tool{
  private Configuration conf;
  private String hdfsFile;
  private String odpsAccessId;
  private String odpsAccessKey;
  private String odpsEndpoint;
  private String odpsProject;
  private String odpsTable;
  private String odpsPartition;
  
  public Hdfs2ODPS(Configuration conf) {
    super(conf);
    setConf(conf);
  }
  
  public static void main(String[] args) throws Exception {
    //System.out.println(Base64.class.getProtectionDomain().getCodeSource().getLocation());
    int res = ToolRunner.run(new Hdfs2ODPS(new Configuration()), args);
    System.exit(res);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  // inherit javadoc
  public Configuration getConf() {
    return this.conf;
  }
  
  public int run(String[] args) throws Exception {
    try {
      parseArgument(args);
    } catch (IllegalArgumentException e) {
      printUsage(e.getMessage());
      return -1;
    }
       
    // create ODPS tunnel
    Account account = new AliyunAccount(odpsAccessId, odpsAccessKey);
    Odps odps = new Odps(account);
    odps.setDefaultProject(odpsProject);
    odps.setEndpoint(odpsEndpoint); 
    
    DataTunnel tunnel = new DataTunnel(odps);
    UploadSession upload;
    if(odpsPartition != null) {
      PartitionSpec spec = new PartitionSpec(odpsPartition);
      upload= tunnel.createUploadSession(odpsProject, odpsTable, spec);
    }
    else
    {
      upload = tunnel.createUploadSession(odpsProject, odpsTable);
    }  
    
    String uploadId = upload.getId();
    conf.set("tunnel.uploadid", uploadId);
    
    // commit all blocks
    int maps = runJob();
    Long[] success = new Long[maps];
    for (int i=0; i<maps; i++) {
      success[i] = (long)i;
    }
    System.out.println("Job finished, total tasks: " + maps);
    upload.commit(success);
    
    return 0;
  }  
  
  private int runJob() throws IOException, InterruptedException, ClassNotFoundException {
    JobConf conf = new JobConf(this.conf);
    conf.setJobName("hdfs2odps");
    conf.setJarByClass(Hdfs2ODPS.class);
    conf.setMapperClass(LoadMapper.class);
    conf.setOutputKeyClass(Text.class);
    conf.setNumReduceTasks(0);
    
    
    FileInputFormat.setInputPaths(conf, this.hdfsFile);
    conf.setOutputFormat(NullOutputFormat.class);
    JobClient.runJob(conf);
    return conf.getNumMapTasks();
  }
  
  
  private static void printUsage(String msg) {
    System.out.println(
          "Usage: hdfs2odps -h hadoop_path \\\n"        
        + "                 -t odps_table \\\n"
        + "                 -c odps_config_file \\\n"
        + "                  [-p odps_partition] \\\n" 
        );
    if (msg != null) {
      System.out.println(msg);
    }
  }
  
  private void parseArgument(String[] allArgs) {    
    String[] args = new GenericOptionsParser(conf, allArgs).getRemainingArgs(); 

    for (int idx = 0; idx < args.length; idx++) {
      if ("-h".equals(args[idx])) {
        if (++idx ==  args.length) {
          throw new IllegalArgumentException("hadoop file not specified in -h");
        }
        this.hdfsFile = args[idx];
      } 
      else if ("-t".equals(args[idx])) {
        if (++idx ==  args.length) {
          throw new IllegalArgumentException("ODPS table not specified in -o");
        }
        this.odpsTable = args[idx];
        conf.set("odps.table", odpsTable);
      } 
      else if ("-c".equals(args[idx])) {
        if (++idx ==  args.length) {
          throw new IllegalArgumentException(
              "ODPS configuration file not specified in -c");
        }
        try {
          InputStream is = new FileInputStream(args[idx]);
          Properties props = new Properties();
          props.load(is);
          
          DesUtils util = new DesUtils();
          this.odpsAccessId = props.getProperty("access.id");
          this.odpsAccessKey = props.getProperty("access.key");
          this.odpsProject = props.getProperty("default.project");
          this.odpsEndpoint = props.getProperty("endpoint");
          conf.set("odps.accessid", util.encrypt(this.odpsAccessId));
          conf.set("odps.accesskey", util.encrypt(this.odpsAccessKey));
          conf.set("odps.project", odpsProject);
          conf.set("odps.endpoint", odpsEndpoint);
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "Error reading ODPS config file '" + args[idx] + "'.");
        } catch (GeneralSecurityException e) {
          throw new IllegalArgumentException(
              "Error initialize DesUtils.");
        }
      }
      else if ("-p".equals(args[idx])){
        if (++idx ==  args.length) {
          throw new IllegalArgumentException(
              "odps table partition not specified in -p");
        }
        conf.set("odps.partition", args[idx]);
      } 
    }    
  
    if (odpsTable == null) {
      throw new IllegalArgumentException(
          "Missing argument -t dst_table_on_odps");
    }    
    if (odpsAccessId == null || odpsAccessKey == null || 
        odpsProject == null || odpsEndpoint == null) {
      throw new IllegalArgumentException(
          "ODPS conf not set, please check -c odps.conf");
    }
  }
  
}
