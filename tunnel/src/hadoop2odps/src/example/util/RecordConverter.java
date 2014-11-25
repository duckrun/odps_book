package example.util;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;

public class RecordConverter {

  TableSchema schema;
  String nullTag;
  SimpleDateFormat dateFormater;
  
  DecimalFormat doubleFormat ;
  public static String DEFAULT_DATE_FORMAT_PATTERN = "yyyyMMddHHmmss";


  public RecordConverter(TableSchema schema, String nullTag, String dateFormat, String tz) {

    this.schema = schema;
    this.nullTag = nullTag;

    if (dateFormat == null) {
      this.dateFormater = new SimpleDateFormat(DEFAULT_DATE_FORMAT_PATTERN);
    } else {
      dateFormater = new SimpleDateFormat(dateFormat);
    }
    dateFormater.setLenient(false);
    dateFormater.setTimeZone(TimeZone.getTimeZone(tz == null ?"GMT":tz));
    
    doubleFormat = new DecimalFormat();
    doubleFormat.setMinimumFractionDigits(0);
    doubleFormat.setMaximumFractionDigits(20);
  }

  /**
   * record to String array
   * */
  public String[] format(Record r) {
    int cols = schema.getColumns().size();
    String[] line = new String[cols];
    String colValue = null;
    for (int i = 0; i < cols; i++) {
      Column column = schema.getColumn(i);
      OdpsType t = column.getType();
      switch (t) {
        case BIGINT: {
          Long v = r.getBigint(i);
          colValue = v == null ? null : v.toString();
          break;
        }
        case DOUBLE: {
          Double v = r.getDouble(i);
          if (v == null){
            colValue = null;
          }else{
            colValue = doubleFormat.format(v).replaceAll(",", "");
          }
          break;
        }
        case DATETIME: {
          Date v = r.getDatetime(i);
          if (v == null) {
            colValue = null;
          } else {
            colValue = dateFormater.format(v);
          }
          break;
        }
        case BOOLEAN: {
          Boolean v = r.getBoolean(i);
          colValue = v == null ? null : v.toString();
          break;
        }
        case STRING: {
          String v = r.getString(i);
          colValue = (v == null ? null : v.toString());
          break;
        }
        default:
          throw new RuntimeException("Unknown column type: " + t);
      }

      if (colValue == null) {
        line[i] = nullTag;
      } else {
        line[i] = colValue;
      }
    }
    return line;
  }

  /**
   * String array to record
   * */
  public Record parse(String[] line, long n) throws RuntimeException {

    if (line == null) {
      return null;
    }
    StringBuilder errorBuild = new StringBuilder();
    int columnCnt = schema.getColumns().size();
    if (line.length != columnCnt) {
      errorBuild.append("columns mismatch - line " + n);
    }
    
    Column[] cols = new Column[columnCnt];
    for(int i=0; i<columnCnt; ++i) {
      Column c = new Column(schema.getColumn(i).getName(), schema.getColumn(i).getType());
      cols[i] = c;
    }

    ArrayRecord r = new ArrayRecord(cols);
    int idx = 0;
    for (String v : line) {
      if (v.equals(nullTag)) {
        idx++;
        continue;
      }
      if (idx >= columnCnt) {
        break;
      }
      OdpsType type = schema.getColumn(idx).getType();
      String eMsg = "";
      try {
        switch (type) {
          case BIGINT:
            r.setBigint(idx, Long.valueOf(v));
            break;
          case DOUBLE:
            r.setDouble(idx, Double.valueOf(v));
            break;
          case DATETIME:
            r.setDatetime(idx, dateFormater.parse(v));
            break;          
          case BOOLEAN:
            v = v.trim().toLowerCase();
            if (v.equals("true") || v.equals("false")) {
              r.setBoolean(idx, v.equals("true") ? true : false);
            } else if (v.equals("0") || v.equals("1")) {
              r.setBoolean(idx, v.equals("1") ? true : false);
            } else {
              eMsg = "invalid boolean type, expect: 'true'|'false'|'0'|'1'";
              throw new RuntimeException(eMsg);
            }

            break;
          case STRING:            
              r.setString(idx, v);
            
            break;          
          default:
            eMsg = "Unknown column type";
            throw new RuntimeException(eMsg);
        }
      } catch (Exception e) {
        errorBuild.append("format error - line " + n);
      }
      idx++;
    }

    String errorMsg = errorBuild.toString();
    if (!errorMsg.isEmpty()) {
      throw new RuntimeException(errorMsg);
    }
    return r;
  }

}
