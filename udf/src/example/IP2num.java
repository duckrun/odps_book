package example;
import com.aliyun.odps.udf.UDF;

public final class IP2num extends UDF{
  public Long evaluate(String ip) {
    long result = 0;
    String[] ipArray = ip.split("\\.");
    for(int i=3; i>=0; i--) {
      long n = Long.parseLong(ipArray[3-i]);
      result |= n << (i*8);
    }
    return result;
  }    
}

