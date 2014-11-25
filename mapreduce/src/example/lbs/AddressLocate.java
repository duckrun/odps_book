package example.lbs;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import ch.hsr.geohash.GeoHash;

import com.aliyun.odps.counter.Counter;
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

import example.util.GeoUtil;
import example.util.Pair;

public class AddressLocate {

  private static String TABLE_SRC;  // values are the center point
  private static String TABLE_POI;  // POI table
  private static String TABLE_OUT;
  private static int PRECISION = 5;
  private static long DISTANCE = 1000;
  
  enum DataCounter {
    CACHE,
    POI,
    SRC
  }
	
  public static class LocateMapper extends MapperBase {
    private Record key;
    private Record value;    
    
    @Override
    public void setup(TaskContext context) throws IOException {
      key = context.createMapOutputKeyRecord();
      value = context.createMapOutputValueRecord();
      TABLE_SRC = context.getJobConf().get("TABLE_SRC");
      TABLE_POI = context.getJobConf().get("TABLE_POI");
      PRECISION = context.getJobConf().getInt("PRECISION", 5);
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
      // id(string), latitude, longitude       
      Double lat = record.getDouble(1);
      Double lng = record.getDouble(2);
      
      //src or poi: src should come first in reduce      
      if(context.getInputTableInfo().getTableName().equals(TABLE_SRC)) {       
        key.set(1, 1L);
      }
      else if(context.getInputTableInfo().getTableName().equals(TABLE_POI)){  
        key.set(1, 2L);
      } 
      String hash = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, PRECISION);
      key.set(0, hash);
      value.set(0, record.get(0));
      value.setDouble(1, lat);
      value.setDouble(2, lng);
      context.write(key, value);
    }
  }
  
  // INPUT	: (hash, [1|2]) (id, lat, lng)
  // OUTPUT	: src_id, poi_id, distance
  public static class LocateReducer extends ReducerBase {
    private Record result = null; 

    // maintain: id, <lat, lng> (from src)
    HashMap<String, Pair<Double, Double>> cache = null;
    private String lastHash = null;
    
    Counter cacheCounter;
    Counter poiCounter;
    Counter srcCounter;
    
    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
      DISTANCE = context.getJobConf().getLong("DISTANCE", 1000);  // default 1km
      cache = new HashMap<String, Pair<Double, Double>>();
      cacheCounter = context.getCounter(DataCounter.CACHE);
      poiCounter = context.getCounter(DataCounter.POI);
      srcCounter = context.getCounter(DataCounter.SRC);
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      
      while (values.hasNext()) {
        Record val = values.next(); 
        String hash = key.getString(0);
    
		System.out.println("key:" + key.getString(0) + "," + key.getBigint(1) );
		System.out.println("value:" + val.getString(0) + "," + val.getDouble(1) + "," + val.getDouble(2));

        if(key.getBigint(1) == 1L) { 	// from src 
        	srcCounter.increment(1);
          if( null == lastHash || lastHash != hash ) {
            cache.clear();          	
            lastHash = hash;            
          }
          // since the hash value is a rectangle, different src_ids may get the same hash value
          cache.put(val.getString(0), new Pair<Double, Double>(val.getDouble(1), val.getDouble(2)));
        }
        else {  	// from poi
          // get distance   
        	poiCounter.increment(1);
          for(String k: cache.keySet()) {
          	cacheCounter.increment(1);          	
            Pair<Double, Double> p = (Pair<Double, Double>) cache.get(k);
            double d = GeoUtil.getPointDistance(p.getLeft(), p.getRight(), val.getDouble(1), val.getDouble(2));

            // output
            if(d <= DISTANCE) {
              result.set(0, k);
              result.set(1, val.getString(0));
              result.set(2, d);
              context.write(result);  
            }
          }          
        }
      }      
    }
  }
  
  public static void main(String[] args) throws Exception {
  	try {
      parseArgument(args);
    } catch (IllegalArgumentException e) {
      printUsage(e.getMessage());
      System.exit(2);
    }    

    JobConf job = new JobConf();

    job.setMapperClass(LocateMapper.class);
    job.setReducerClass(LocateReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("hash:string,type:bigint"));    
    job.setMapOutputValueSchema(SchemaUtils.fromString("id:string,lat:double,lng:double"));

    InputUtils.addTable(TableInfo.builder().tableName(TABLE_SRC).build(), job);
    InputUtils.addTable(TableInfo.builder().tableName(TABLE_POI).build(), job);   
    OutputUtils.addTable(TableInfo.builder().tableName(TABLE_OUT).build(), job);
    
    // precision
    job.setInt("PRESICION", PRECISION);
    job.setLong("DISTANCE", DISTANCE);
    
    job.set("TABLE_SRC", TABLE_SRC);
    job.set("TABLE_POI", TABLE_POI);
    
    JobClient.runJob(job);
  }  
  
  private static void printUsage(String msg) {
    System.out.println(
          "Usage: AddressLocate -src table_src \\\n"
        + "                  		-poi table_poi\\\n"
        + "                  		-out table_out \\\n"
        + "                  		[-p precision] \\\n"
        + "                  		[-d distance] \\\n" 
        );
    if (msg != null) {
      System.out.println(msg);
    }
  }
  
  private static void parseArgument(String[] args) {
    for (int i = 0; i < args.length; i++) {
    	if ("-src".equals(args[i])) {
        if (++i ==  args.length) {
          throw new IllegalArgumentException("src table not specified in -src");
        }
        TABLE_SRC = args[i];
      }
    	else if ("-poi".equals(args[i])) {
        if (++i ==  args.length) {
          throw new IllegalArgumentException("poi table not specified in -poi");
        }
        TABLE_POI = args[i];
      } 
    	else if ("-out".equals(args[i])) {
    		if (++i ==  args.length) {
          throw new IllegalArgumentException("output table not specified in -out");
        }
        TABLE_OUT = args[i];
    	}
      else if ("-p".equals(args[i])) {
        if (++i ==  args.length) {
          throw new IllegalArgumentException(
              "precision not specified in -c");
        }
        PRECISION = Integer.parseInt(args[i]);
      }
      else if ("-d".equals(args[i])) {
        if (++i ==  args.length) {
          throw new IllegalArgumentException(
              "distance not specified in -c");
        }
        DISTANCE = Long.parseLong(args[i]);
      }
    }
    
    // those params are must
  	if(TABLE_SRC == null) {
  		throw new IllegalArgumentException(
          "Missing argument -src table_src_in");
  	}
  	if (TABLE_POI == null) {
      throw new IllegalArgumentException(
          "Missing argument -poi table_poi_in");
    }  
  	if (TABLE_OUT == null) {
      throw new IllegalArgumentException(
          "Missing argument -out table_out");
    }    
  }
    
  
}
