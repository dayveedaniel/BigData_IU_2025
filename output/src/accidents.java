// ORM class for table 'accidents'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sun May 18 16:33:46 MSK 2025
// For connector: org.apache.sqoop.manager.PostgresqlManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.sqoop.lib.JdbcWritableBridge;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.lib.FieldFormatter;
import org.apache.sqoop.lib.RecordParser;
import org.apache.sqoop.lib.BooleanParser;
import org.apache.sqoop.lib.BlobRef;
import org.apache.sqoop.lib.ClobRef;
import org.apache.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class accidents extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.id = (String)value;
      }
    });
    setters.put("source", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.source = (String)value;
      }
    });
    setters.put("severity", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.severity = (Integer)value;
      }
    });
    setters.put("start_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.start_time = (java.sql.Timestamp)value;
      }
    });
    setters.put("end_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.end_time = (java.sql.Timestamp)value;
      }
    });
    setters.put("distance_mi", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.distance_mi = (java.math.BigDecimal)value;
      }
    });
    setters.put("description", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.description = (String)value;
      }
    });
    setters.put("location_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.location_id = (Long)value;
      }
    });
    setters.put("weather_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.weather_id = (Long)value;
      }
    });
    setters.put("twilight_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.twilight_id = (Integer)value;
      }
    });
    setters.put("road_feat_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.road_feat_id = (Integer)value;
      }
    });
  }
  public accidents() {
    init0();
  }
  private String id;
  public String get_id() {
    return id;
  }
  public void set_id(String id) {
    this.id = id;
  }
  public accidents with_id(String id) {
    this.id = id;
    return this;
  }
  private String source;
  public String get_source() {
    return source;
  }
  public void set_source(String source) {
    this.source = source;
  }
  public accidents with_source(String source) {
    this.source = source;
    return this;
  }
  private Integer severity;
  public Integer get_severity() {
    return severity;
  }
  public void set_severity(Integer severity) {
    this.severity = severity;
  }
  public accidents with_severity(Integer severity) {
    this.severity = severity;
    return this;
  }
  private java.sql.Timestamp start_time;
  public java.sql.Timestamp get_start_time() {
    return start_time;
  }
  public void set_start_time(java.sql.Timestamp start_time) {
    this.start_time = start_time;
  }
  public accidents with_start_time(java.sql.Timestamp start_time) {
    this.start_time = start_time;
    return this;
  }
  private java.sql.Timestamp end_time;
  public java.sql.Timestamp get_end_time() {
    return end_time;
  }
  public void set_end_time(java.sql.Timestamp end_time) {
    this.end_time = end_time;
  }
  public accidents with_end_time(java.sql.Timestamp end_time) {
    this.end_time = end_time;
    return this;
  }
  private java.math.BigDecimal distance_mi;
  public java.math.BigDecimal get_distance_mi() {
    return distance_mi;
  }
  public void set_distance_mi(java.math.BigDecimal distance_mi) {
    this.distance_mi = distance_mi;
  }
  public accidents with_distance_mi(java.math.BigDecimal distance_mi) {
    this.distance_mi = distance_mi;
    return this;
  }
  private String description;
  public String get_description() {
    return description;
  }
  public void set_description(String description) {
    this.description = description;
  }
  public accidents with_description(String description) {
    this.description = description;
    return this;
  }
  private Long location_id;
  public Long get_location_id() {
    return location_id;
  }
  public void set_location_id(Long location_id) {
    this.location_id = location_id;
  }
  public accidents with_location_id(Long location_id) {
    this.location_id = location_id;
    return this;
  }
  private Long weather_id;
  public Long get_weather_id() {
    return weather_id;
  }
  public void set_weather_id(Long weather_id) {
    this.weather_id = weather_id;
  }
  public accidents with_weather_id(Long weather_id) {
    this.weather_id = weather_id;
    return this;
  }
  private Integer twilight_id;
  public Integer get_twilight_id() {
    return twilight_id;
  }
  public void set_twilight_id(Integer twilight_id) {
    this.twilight_id = twilight_id;
  }
  public accidents with_twilight_id(Integer twilight_id) {
    this.twilight_id = twilight_id;
    return this;
  }
  private Integer road_feat_id;
  public Integer get_road_feat_id() {
    return road_feat_id;
  }
  public void set_road_feat_id(Integer road_feat_id) {
    this.road_feat_id = road_feat_id;
  }
  public accidents with_road_feat_id(Integer road_feat_id) {
    this.road_feat_id = road_feat_id;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof accidents)) {
      return false;
    }
    accidents that = (accidents) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.source == null ? that.source == null : this.source.equals(that.source));
    equal = equal && (this.severity == null ? that.severity == null : this.severity.equals(that.severity));
    equal = equal && (this.start_time == null ? that.start_time == null : this.start_time.equals(that.start_time));
    equal = equal && (this.end_time == null ? that.end_time == null : this.end_time.equals(that.end_time));
    equal = equal && (this.distance_mi == null ? that.distance_mi == null : this.distance_mi.equals(that.distance_mi));
    equal = equal && (this.description == null ? that.description == null : this.description.equals(that.description));
    equal = equal && (this.location_id == null ? that.location_id == null : this.location_id.equals(that.location_id));
    equal = equal && (this.weather_id == null ? that.weather_id == null : this.weather_id.equals(that.weather_id));
    equal = equal && (this.twilight_id == null ? that.twilight_id == null : this.twilight_id.equals(that.twilight_id));
    equal = equal && (this.road_feat_id == null ? that.road_feat_id == null : this.road_feat_id.equals(that.road_feat_id));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof accidents)) {
      return false;
    }
    accidents that = (accidents) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.source == null ? that.source == null : this.source.equals(that.source));
    equal = equal && (this.severity == null ? that.severity == null : this.severity.equals(that.severity));
    equal = equal && (this.start_time == null ? that.start_time == null : this.start_time.equals(that.start_time));
    equal = equal && (this.end_time == null ? that.end_time == null : this.end_time.equals(that.end_time));
    equal = equal && (this.distance_mi == null ? that.distance_mi == null : this.distance_mi.equals(that.distance_mi));
    equal = equal && (this.description == null ? that.description == null : this.description.equals(that.description));
    equal = equal && (this.location_id == null ? that.location_id == null : this.location_id.equals(that.location_id));
    equal = equal && (this.weather_id == null ? that.weather_id == null : this.weather_id.equals(that.weather_id));
    equal = equal && (this.twilight_id == null ? that.twilight_id == null : this.twilight_id.equals(that.twilight_id));
    equal = equal && (this.road_feat_id == null ? that.road_feat_id == null : this.road_feat_id.equals(that.road_feat_id));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readString(1, __dbResults);
    this.source = JdbcWritableBridge.readString(2, __dbResults);
    this.severity = JdbcWritableBridge.readInteger(3, __dbResults);
    this.start_time = JdbcWritableBridge.readTimestamp(4, __dbResults);
    this.end_time = JdbcWritableBridge.readTimestamp(5, __dbResults);
    this.distance_mi = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.description = JdbcWritableBridge.readString(7, __dbResults);
    this.location_id = JdbcWritableBridge.readLong(8, __dbResults);
    this.weather_id = JdbcWritableBridge.readLong(9, __dbResults);
    this.twilight_id = JdbcWritableBridge.readInteger(10, __dbResults);
    this.road_feat_id = JdbcWritableBridge.readInteger(11, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.id = JdbcWritableBridge.readString(1, __dbResults);
    this.source = JdbcWritableBridge.readString(2, __dbResults);
    this.severity = JdbcWritableBridge.readInteger(3, __dbResults);
    this.start_time = JdbcWritableBridge.readTimestamp(4, __dbResults);
    this.end_time = JdbcWritableBridge.readTimestamp(5, __dbResults);
    this.distance_mi = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.description = JdbcWritableBridge.readString(7, __dbResults);
    this.location_id = JdbcWritableBridge.readLong(8, __dbResults);
    this.weather_id = JdbcWritableBridge.readLong(9, __dbResults);
    this.twilight_id = JdbcWritableBridge.readInteger(10, __dbResults);
    this.road_feat_id = JdbcWritableBridge.readInteger(11, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(id, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(source, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(severity, 3 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeTimestamp(start_time, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(end_time, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(distance_mi, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(description, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(location_id, 8 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(weather_id, 9 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeInteger(twilight_id, 10 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(road_feat_id, 11 + __off, 5, __dbStmt);
    return 11;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(id, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(source, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(severity, 3 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeTimestamp(start_time, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(end_time, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(distance_mi, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(description, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(location_id, 8 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(weather_id, 9 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeInteger(twilight_id, 10 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(road_feat_id, 11 + __off, 5, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.source = null;
    } else {
    this.source = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.severity = null;
    } else {
    this.severity = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.start_time = null;
    } else {
    this.start_time = new Timestamp(__dataIn.readLong());
    this.start_time.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.end_time = null;
    } else {
    this.end_time = new Timestamp(__dataIn.readLong());
    this.end_time.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.distance_mi = null;
    } else {
    this.distance_mi = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.description = null;
    } else {
    this.description = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.location_id = null;
    } else {
    this.location_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.weather_id = null;
    } else {
    this.weather_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.twilight_id = null;
    } else {
    this.twilight_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.road_feat_id = null;
    } else {
    this.road_feat_id = Integer.valueOf(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, id);
    }
    if (null == this.source) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, source);
    }
    if (null == this.severity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.severity);
    }
    if (null == this.start_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.start_time.getTime());
    __dataOut.writeInt(this.start_time.getNanos());
    }
    if (null == this.end_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.end_time.getTime());
    __dataOut.writeInt(this.end_time.getNanos());
    }
    if (null == this.distance_mi) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.distance_mi, __dataOut);
    }
    if (null == this.description) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, description);
    }
    if (null == this.location_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.location_id);
    }
    if (null == this.weather_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.weather_id);
    }
    if (null == this.twilight_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.twilight_id);
    }
    if (null == this.road_feat_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.road_feat_id);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, id);
    }
    if (null == this.source) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, source);
    }
    if (null == this.severity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.severity);
    }
    if (null == this.start_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.start_time.getTime());
    __dataOut.writeInt(this.start_time.getNanos());
    }
    if (null == this.end_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.end_time.getTime());
    __dataOut.writeInt(this.end_time.getNanos());
    }
    if (null == this.distance_mi) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.distance_mi, __dataOut);
    }
    if (null == this.description) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, description);
    }
    if (null == this.location_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.location_id);
    }
    if (null == this.weather_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.weather_id);
    }
    if (null == this.twilight_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.twilight_id);
    }
    if (null == this.road_feat_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.road_feat_id);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"\\N":id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(source==null?"\\N":source, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(severity==null?"\\N":"" + severity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_time==null?"\\N":"" + start_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_time==null?"\\N":"" + end_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(distance_mi==null?"\\N":distance_mi.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(description==null?"\\N":description, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(location_id==null?"\\N":"" + location_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(weather_id==null?"\\N":"" + weather_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(twilight_id==null?"\\N":"" + twilight_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(road_feat_id==null?"\\N":"" + road_feat_id, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"\\N":id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(source==null?"\\N":source, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(severity==null?"\\N":"" + severity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_time==null?"\\N":"" + start_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_time==null?"\\N":"" + end_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(distance_mi==null?"\\N":distance_mi.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(description==null?"\\N":description, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(location_id==null?"\\N":"" + location_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(weather_id==null?"\\N":"" + weather_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(twilight_id==null?"\\N":"" + twilight_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(road_feat_id==null?"\\N":"" + road_feat_id, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.id = null; } else {
      this.id = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.source = null; } else {
      this.source = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.severity = null; } else {
      this.severity = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.start_time = null; } else {
      this.start_time = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.end_time = null; } else {
      this.end_time = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.distance_mi = null; } else {
      this.distance_mi = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.description = null; } else {
      this.description = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.location_id = null; } else {
      this.location_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.weather_id = null; } else {
      this.weather_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.twilight_id = null; } else {
      this.twilight_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.road_feat_id = null; } else {
      this.road_feat_id = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.id = null; } else {
      this.id = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.source = null; } else {
      this.source = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.severity = null; } else {
      this.severity = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.start_time = null; } else {
      this.start_time = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.end_time = null; } else {
      this.end_time = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.distance_mi = null; } else {
      this.distance_mi = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.description = null; } else {
      this.description = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.location_id = null; } else {
      this.location_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.weather_id = null; } else {
      this.weather_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.twilight_id = null; } else {
      this.twilight_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.road_feat_id = null; } else {
      this.road_feat_id = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    accidents o = (accidents) super.clone();
    o.start_time = (o.start_time != null) ? (java.sql.Timestamp) o.start_time.clone() : null;
    o.end_time = (o.end_time != null) ? (java.sql.Timestamp) o.end_time.clone() : null;
    return o;
  }

  public void clone0(accidents o) throws CloneNotSupportedException {
    o.start_time = (o.start_time != null) ? (java.sql.Timestamp) o.start_time.clone() : null;
    o.end_time = (o.end_time != null) ? (java.sql.Timestamp) o.end_time.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("source", this.source);
    __sqoop$field_map.put("severity", this.severity);
    __sqoop$field_map.put("start_time", this.start_time);
    __sqoop$field_map.put("end_time", this.end_time);
    __sqoop$field_map.put("distance_mi", this.distance_mi);
    __sqoop$field_map.put("description", this.description);
    __sqoop$field_map.put("location_id", this.location_id);
    __sqoop$field_map.put("weather_id", this.weather_id);
    __sqoop$field_map.put("twilight_id", this.twilight_id);
    __sqoop$field_map.put("road_feat_id", this.road_feat_id);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("source", this.source);
    __sqoop$field_map.put("severity", this.severity);
    __sqoop$field_map.put("start_time", this.start_time);
    __sqoop$field_map.put("end_time", this.end_time);
    __sqoop$field_map.put("distance_mi", this.distance_mi);
    __sqoop$field_map.put("description", this.description);
    __sqoop$field_map.put("location_id", this.location_id);
    __sqoop$field_map.put("weather_id", this.weather_id);
    __sqoop$field_map.put("twilight_id", this.twilight_id);
    __sqoop$field_map.put("road_feat_id", this.road_feat_id);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
