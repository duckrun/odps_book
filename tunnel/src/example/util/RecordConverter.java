package example.util;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;

public class RecordConverter {

	private TableSchema schema;
	private String nullTag;
	private SimpleDateFormat dateFormater;

	private DecimalFormat doubleFormat;
	private static String DEFAULT_DATE_FORMAT_PATTERN = "yyyyMMddHHmmss";

	public RecordConverter(TableSchema schema, String nullTag, String dateFormat,
			String tz) {

		this.schema = schema;
		this.nullTag = nullTag;

		if (dateFormat == null) {
			this.dateFormater = new SimpleDateFormat(DEFAULT_DATE_FORMAT_PATTERN);
		} else {
			dateFormater = new SimpleDateFormat(dateFormat);
		}
		dateFormater.setLenient(false);
		dateFormater.setTimeZone(TimeZone.getTimeZone(tz == null ? "GMT" : tz));

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
				if (v == null) {
					colValue = null;
				} else {
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
	 * @throws ParseException 
	 * */
	public Record parse(String[] line){

		if (line == null) {
			return null;
		}

		int columnCnt = schema.getColumns().size();
		Column[] cols = new Column[columnCnt];
		for (int i = 0; i < columnCnt; ++i) {
			Column c = new Column(schema.getColumn(i).getName(), 
					schema.getColumn(i).getType());					
			cols[i] = c;
		}

		ArrayRecord r = new ArrayRecord(cols);
		int i = 0;
		for (String v : line) {
			if (v.equals(nullTag)) {
				i++;
				continue;
			}
			if (i >= columnCnt) {
				break;
			}
			OdpsType type = schema.getColumn(i).getType();
			switch (type) {
			case BIGINT:
				r.setBigint(i, Long.valueOf(v));
				break;
			case DOUBLE:
				r.setDouble(i, Double.valueOf(v));
				break;
			case DATETIME:
				try {
					r.setDatetime(i, dateFormater.parse(v));
				} catch (ParseException e) {
					throw new RuntimeException(e.getMessage());
				}
				break;
			case BOOLEAN:
				v = v.trim().toLowerCase();
				if (v.equals("true") || v.equals("false")) {
					r.setBoolean(i, v.equals("true") ? true : false);
				} else if (v.equals("0") || v.equals("1")) {
					r.setBoolean(i, v.equals("1") ? true : false);
				} else {
					throw new RuntimeException(
							"Invalid boolean type, expect: true|false|0|1");
				}
				break;
			case STRING:
				r.setString(i, v);
				break;
			default:
				throw new RuntimeException("Unknown column type");
			}
			i++;
		}
		return r;
	}

}
