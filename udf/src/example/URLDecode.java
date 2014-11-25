package example;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import com.aliyun.odps.udf.UDF;

public final class URLDecode extends UDF{
  public String evaluate(String url) throws UnsupportedEncodingException {  
    if(url == null || url.isEmpty()) {
        return "";
    }
    return URLDecoder.decode(url, "UTF-8");
  }    
}
