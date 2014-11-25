package example.upload;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Pattern;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.UploadSession;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.DataTunnel;
import com.aliyun.odps.tunnel.TunnelException;

import example.util.RecordConverter;

public class FileUpload {
  
  private static String accessId;
  private static String accessKey;  
  private static String endpoint;
  private static String project;
  private static String table;
  private static String partition;
  private static String fieldDelimeter;
  private static String file;

  public static void main(String args[]) {
  	try {
      parseArgument(args);
    } catch (IllegalArgumentException e) {
      printUsage(e.getMessage());
      System.exit(2);
    }    
    
    Account account = new AliyunAccount(accessId, accessKey);
    Odps odps = new Odps(account);
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);  
    
    BufferedReader br = null;
    try {      
      DataTunnel tunnel = new DataTunnel(odps);
      
      UploadSession uploadSession = null;
      if(partition != null) {
        PartitionSpec spec = new PartitionSpec(partition);
        uploadSession= tunnel.createUploadSession(project, table, spec);
      }
      else
      {
        uploadSession= tunnel.createUploadSession(project, table);
      }        
         
      Long blockid = (long) 0;
      RecordWriter recordWriter = uploadSession.openRecordWriter(blockid, true);
      Record record = uploadSession.newRecord();
      
      TableSchema schema = uploadSession.getSchema();      
      RecordConverter converter = new RecordConverter(schema, "NULL", null, null);      
      br = new BufferedReader(new FileReader(file));
      Pattern pattern = Pattern.compile(fieldDelimeter);
      
      String line = null;
      while ((line = br.readLine()) != null) {
        String[] items=pattern.split(line,0);
        record = converter.parse(items);
        recordWriter.write(record);
      }       
      recordWriter.close();      
      Long[] blocks = {blockid};
      uploadSession.commit(blocks);         
    } catch (TunnelException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }finally {
      try {
        if (br != null)
          br.close();
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
  } 
  
  private static void printUsage(String msg) {
    System.out.println(
          "Usage: FileUpload -f file \\\n"
        + "                  -t table\\\n"
        + "                  -c config_file \\\n"
        + "                  [-p partition] \\\n"
        + "                  [-fd field_delimiter] \\\n" 
        );
    if (msg != null) {
      System.out.println(msg);
    }
  }
  
  private static void parseArgument(String[] args) {
    for (int i = 0; i < args.length; i++) {
    	if ("-f".equals(args[i])) {
        if (++i ==  args.length) {
          throw new IllegalArgumentException("source file not specified in -f");
        }
        file = args[i];
      }
    	else if ("-t".equals(args[i])) {
        if (++i ==  args.length) {
          throw new IllegalArgumentException("ODPS table not specified in -t");
        }
        table = args[i];
      } 
      else if ("-c".equals(args[i])) {
        if (++i ==  args.length) {
          throw new IllegalArgumentException(
              "ODPS configuration file not specified in -c");
        }
        try {
          InputStream is = new FileInputStream(args[i]);
          Properties props = new Properties();
          props.load(is);
          accessId = props.getProperty("access.id");
          accessKey = props.getProperty("access.key");
          project = props.getProperty("default.project");
          endpoint = props.getProperty("endpoint");          
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "Error reading ODPS config file '" + args[i] + "'.");
        }
      }
      else if ("-p".equals(args[i])){
        if (++i ==  args.length) {
          throw new IllegalArgumentException(
              "odps table partition not specified in -p");
        }
        partition = args[i];
      }  
    } 
    // those params are must
  	if(file == null) {
  		throw new IllegalArgumentException(
          "Missing argument -f file");
  	}
  	if (table == null) {
      throw new IllegalArgumentException(
          "Missing argument -t table");
    }  
  	
    if (accessId == null || accessKey == null || 
    		project == null || endpoint == null) {
      throw new IllegalArgumentException(
          "ODPS conf not set, please check -c odps.conf");
    }
  }
}