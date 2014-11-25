package example;

import com.aliyun.odps.udf.UDF;

public final class GeoEncode extends UDF{
  public String evaluate(Double latitude, Double longitude) {
    return new GeoHash().encode(latitude, longitude);    
  }
}