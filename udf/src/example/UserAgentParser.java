package example;

import java.util.ArrayList;
import java.util.List;

import nl.bitwalker.useragentutils.UserAgent;

import org.apache.commons.codec.digest.DigestUtils;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

/*
 * useragent: os, os.manufacturer,  os.device.type, os.device.mobile, 
 *            browser, browser.manufacturer, browser.engine
 */
@Resolve({"string->string,string,string,string,string,string,string,string"})
public class UserAgentParser extends UDTF {
  
  @Override
  public void process(Object[] args) {
    if(args.length == 0) return;
    String agent = (String) args[0];
    
    List<String> result = new ArrayList<String>();
    UserAgent ua = UserAgent.parseUserAgentString(agent);
   
    // os, os.manufacturer,  os.device.type, os.device.mobile
    result.add(ua.getOperatingSystem().toString());  
    result.add(ua.getOperatingSystem().getManufacturer().getName());
    result.add(ua.getOperatingSystem().getDeviceType().getName());
    result.add(String.valueOf(ua.getOperatingSystem().isMobileDevice())); // true/false
    
    //browser,  browser.manufacturer, browser.engine
    result.add(ua.getBrowser().toString());
    result.add(ua.getBrowser().getManufacturer().getName());
    result.add(ua.getBrowser().getRenderingEngine().toString());
    
    // md5 of all info, uniquely identify this info(record)
    String info = ""; 
    for(String part: result) {
      info += part;
    }
    result.add(DigestUtils.md5Hex(info));

    try {
      forward(result.toArray());
    } catch (UDFException e) {
      e.printStackTrace();
    }
    
  }
}
