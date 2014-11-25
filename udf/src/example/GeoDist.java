package example;

import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.UDFException;

public final class GeoDist extends UDF{
  
  private static final double EARTH_RADIUS = 6378.137;
  
  public Double evaluate(Double lat1, Double lng1, Double lat2, Double lng2) throws UDFException {
  	if(lat1 == null || lng1 == null || lat2 == null || lng2 == null) {
  		throw new UDFException("some param is NULL");
  	}
    double s = 0 ;
    
    double radlat1 = radian(lat1);
    
    double ratlat2 = radian(lat2);
    double a = radian(lat1) - radian(lat2);
    double b = radian(lng1) - radian(lng2);
    
    s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2)
        + Math.cos(radlat1)*Math.cos(ratlat2)*Math.pow(Math.sin(b/2), 2)));
    s = s * EARTH_RADIUS;     
    s = Math.round(s*1000); //m
    
    return s;  
  } 
  
  private static double radian(double d){
    return (d*Math.PI)/180.00;
  }
}