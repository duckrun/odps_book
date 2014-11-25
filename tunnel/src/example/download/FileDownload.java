package example.download;

import java.io.File;
import java.io.FileOutputStream;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.DataTunnel;
import com.aliyun.odps.tunnel.DownloadSession;

import example.util.RecordConverter;

public class FileDownload {
  private static String accessId = "<accessId>";
  private static String accessKey = "<accessKey>";  
  private static String endpoint = "http://service.odps.aliyun.com/api";
  private static String project = "odps_book";
  private static String table = "";
  private static String partition = null;
  private static String fieldDelimeter = "\t"; 
  private static String lineDelimiter = "\n";
  
  public static void main(String args[]) throws Exception {
    if (args.length <2) {
      System.err.println("Usage: FileDownload <file> <table> [partition] [fieldDelimeter]");
      System.exit(2);
    }
    String filename = args[0];
    table = args[1];
    if(args.length > 2 ) {
      partition = args[2];
      if(args.length > 3) {
        fieldDelimeter = args[3];
      }
    }    
    File file = new File(filename);
    
    Account account = new AliyunAccount(accessId, accessKey);
    Odps odps = new Odps(account);
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint); 
    
    DataTunnel tunnel = new DataTunnel(odps);
    
    DownloadSession session;
    if(partition != null) {
      PartitionSpec spec = new PartitionSpec(partition);
      session= tunnel.createDownloadSession(project, table, spec);
    }
    else
    {
      session= tunnel.createDownloadSession(project, table);
    }        
    
    FileOutputStream out = new FileOutputStream(file);
    
    RecordReader reader = session.openRecordReader(0L, session.getRecordCount(), true);
    TableSchema schema = session.getSchema();   
    Record record;
    
    RecordConverter converter = new RecordConverter(schema, "NULL", null, null);
    String[] items = new String[schema.getColumns().size()]; 
    
    while ((record = reader.read()) != null) {
      items = converter.format(record);
      for(int i=0; i<items.length; ++i) {
        if(i>0) out.write(fieldDelimeter.getBytes());           
        out.write(items[i].getBytes());
      } 
      out.write(lineDelimiter.getBytes());
    }
    reader.close();
    out.close();
  }
}


