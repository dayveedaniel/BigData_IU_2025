// ORM class for table 'weather'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sun May 18 16:35:51 MSK 2025
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

public class weather extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        weather.this.id = (Long)value;
      }
    });
    setters.put("airport_code", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        weather.this.airport_code = (String)value;
      }
    });
    setters.put("weather_timestamp", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        weather.this.weather_timestamp = (java.sql.Timestamp)value;
      }
    });
    setters.put("temperature_f", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        weather.this.temperature_f = (java.math.BigDecimal)value;
      }
    });
    setters.put("wind_chill_f", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        weather.this.wind_chill_f = (java.math.BigDecimal)value;
      }
    });
    setters.put("humidity_pct", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        weather.this.humidity_pct = (java.math.BigDecimal)value;
      }
    });
    setters.put("pressure_in", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        weather.this.pressure_in = (java.math.BigDecimal)value;
      }
    });
    setters.put("visibility_mi", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        weather.this.visibility_mi = (java.math.BigDecimal)value;
      }
    });
    setters.put("wind_direction", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        weather.this.wind_direction = (String)value;
      }
    });
    setters.put("wind_speed_mph", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        weather.this.wind_speed_mph = (java.math.BigDecimal)value;
      }
    });
    setters.put("precipitation_in", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        weather.this.precipitation_in = (java.math.BigDecimal)value;
      }
    });
    setters.put("weather_condition", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        weather.this.weather_condition = (String)value;
      }
    });
  }
  public weather() {
    init0();
  }
  private Long id;
  public Long get_id() {
    return id;
  }
  public void set_id(Long id) {
    this.id = id;
  }
  public weather with_id(Long id) {
    this.id = id;
    return this;
  }
  private String airport_code;
  public String get_airport_code() {
    return airport_code;
  }
  public void set_airport_code(String airport_code) {
    this.airport_code = airport_code;
  }
  public weather with_airport_code(String airport_code) {
    this.airport_code = airport_code;
    return this;
  }
  private java.sql.Timestamp weather_timestamp;
  public java.sql.Timestamp get_weather_timestamp() {
    return weather_timestamp;
  }
  public void set_weather_timestamp(java.sql.Timestamp weather_timestamp) {
    this.weather_timestamp = weather_timestamp;
  }
  public weather with_weather_timestamp(java.sql.Timestamp weather_timestamp) {
    this.weather_timestamp = weather_timestamp;
    return this;
  }
  private java.math.BigDecimal temperature_f;
  public java.math.BigDecimal get_temperature_f() {
    return temperature_f;
  }
  public void set_temperature_f(java.math.BigDecimal temperature_f) {
    this.temperature_f = temperature_f;
  }
  public weather with_temperature_f(java.math.BigDecimal temperature_f) {
    this.temperature_f = temperature_f;
    return this;
  }
  private java.math.BigDecimal wind_chill_f;
  public java.math.BigDecimal get_wind_chill_f() {
    return wind_chill_f;
  }
  public void set_wind_chill_f(java.math.BigDecimal wind_chill_f) {
    this.wind_chill_f = wind_chill_f;
  }
  public weather with_wind_chill_f(java.math.BigDecimal wind_chill_f) {
    this.wind_chill_f = wind_chill_f;
    return this;
  }
  private java.math.BigDecimal humidity_pct;
  public java.math.BigDecimal get_humidity_pct() {
    return humidity_pct;
  }
  public void set_humidity_pct(java.math.BigDecimal humidity_pct) {
    this.humidity_pct = humidity_pct;
  }
  public weather with_humidity_pct(java.math.BigDecimal humidity_pct) {
    this.humidity_pct = humidity_pct;
    return this;
  }
  private java.math.BigDecimal pressure_in;
  public java.math.BigDecimal get_pressure_in() {
    return pressure_in;
  }
  public void set_pressure_in(java.math.BigDecimal pressure_in) {
    this.pressure_in = pressure_in;
  }
  public weather with_pressure_in(java.math.BigDecimal pressure_in) {
    this.pressure_in = pressure_in;
    return this;
  }
  private java.math.BigDecimal visibility_mi;
  public java.math.BigDecimal get_visibility_mi() {
    return visibility_mi;
  }
  public void set_visibility_mi(java.math.BigDecimal visibility_mi) {
    this.visibility_mi = visibility_mi;
  }
  public weather with_visibility_mi(java.math.BigDecimal visibility_mi) {
    this.visibility_mi = visibility_mi;
    return this;
  }
  private String wind_direction;
  public String get_wind_direction() {
    return wind_direction;
  }
  public void set_wind_direction(String wind_direction) {
    this.wind_direction = wind_direction;
  }
  public weather with_wind_direction(String wind_direction) {
    this.wind_direction = wind_direction;
    return this;
  }
  private java.math.BigDecimal wind_speed_mph;
  public java.math.BigDecimal get_wind_speed_mph() {
    return wind_speed_mph;
  }
  public void set_wind_speed_mph(java.math.BigDecimal wind_speed_mph) {
    this.wind_speed_mph = wind_speed_mph;
  }
  public weather with_wind_speed_mph(java.math.BigDecimal wind_speed_mph) {
    this.wind_speed_mph = wind_speed_mph;
    return this;
  }
  private java.math.BigDecimal precipitation_in;
  public java.math.BigDecimal get_precipitation_in() {
    return precipitation_in;
  }
  public void set_precipitation_in(java.math.BigDecimal precipitation_in) {
    this.precipitation_in = precipitation_in;
  }
  public weather with_precipitation_in(java.math.BigDecimal precipitation_in) {
    this.precipitation_in = precipitation_in;
    return this;
  }
  private String weather_condition;
  public String get_weather_condition() {
    return weather_condition;
  }
  public void set_weather_condition(String weather_condition) {
    this.weather_condition = weather_condition;
  }
  public weather with_weather_condition(String weather_condition) {
    this.weather_condition = weather_condition;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof weather)) {
      return false;
    }
    weather that = (weather) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.airport_code == null ? that.airport_code == null : this.airport_code.equals(that.airport_code));
    equal = equal && (this.weather_timestamp == null ? that.weather_timestamp == null : this.weather_timestamp.equals(that.weather_timestamp));
    equal = equal && (this.temperature_f == null ? that.temperature_f == null : this.temperature_f.equals(that.temperature_f));
    equal = equal && (this.wind_chill_f == null ? that.wind_chill_f == null : this.wind_chill_f.equals(that.wind_chill_f));
    equal = equal && (this.humidity_pct == null ? that.humidity_pct == null : this.humidity_pct.equals(that.humidity_pct));
    equal = equal && (this.pressure_in == null ? that.pressure_in == null : this.pressure_in.equals(that.pressure_in));
    equal = equal && (this.visibility_mi == null ? that.visibility_mi == null : this.visibility_mi.equals(that.visibility_mi));
    equal = equal && (this.wind_direction == null ? that.wind_direction == null : this.wind_direction.equals(that.wind_direction));
    equal = equal && (this.wind_speed_mph == null ? that.wind_speed_mph == null : this.wind_speed_mph.equals(that.wind_speed_mph));
    equal = equal && (this.precipitation_in == null ? that.precipitation_in == null : this.precipitation_in.equals(that.precipitation_in));
    equal = equal && (this.weather_condition == null ? that.weather_condition == null : this.weather_condition.equals(that.weather_condition));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof weather)) {
      return false;
    }
    weather that = (weather) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.airport_code == null ? that.airport_code == null : this.airport_code.equals(that.airport_code));
    equal = equal && (this.weather_timestamp == null ? that.weather_timestamp == null : this.weather_timestamp.equals(that.weather_timestamp));
    equal = equal && (this.temperature_f == null ? that.temperature_f == null : this.temperature_f.equals(that.temperature_f));
    equal = equal && (this.wind_chill_f == null ? that.wind_chill_f == null : this.wind_chill_f.equals(that.wind_chill_f));
    equal = equal && (this.humidity_pct == null ? that.humidity_pct == null : this.humidity_pct.equals(that.humidity_pct));
    equal = equal && (this.pressure_in == null ? that.pressure_in == null : this.pressure_in.equals(that.pressure_in));
    equal = equal && (this.visibility_mi == null ? that.visibility_mi == null : this.visibility_mi.equals(that.visibility_mi));
    equal = equal && (this.wind_direction == null ? that.wind_direction == null : this.wind_direction.equals(that.wind_direction));
    equal = equal && (this.wind_speed_mph == null ? that.wind_speed_mph == null : this.wind_speed_mph.equals(that.wind_speed_mph));
    equal = equal && (this.precipitation_in == null ? that.precipitation_in == null : this.precipitation_in.equals(that.precipitation_in));
    equal = equal && (this.weather_condition == null ? that.weather_condition == null : this.weather_condition.equals(that.weather_condition));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readLong(1, __dbResults);
    this.airport_code = JdbcWritableBridge.readString(2, __dbResults);
    this.weather_timestamp = JdbcWritableBridge.readTimestamp(3, __dbResults);
    this.temperature_f = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.wind_chill_f = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.humidity_pct = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.pressure_in = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.visibility_mi = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.wind_direction = JdbcWritableBridge.readString(9, __dbResults);
    this.wind_speed_mph = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.precipitation_in = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.weather_condition = JdbcWritableBridge.readString(12, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.id = JdbcWritableBridge.readLong(1, __dbResults);
    this.airport_code = JdbcWritableBridge.readString(2, __dbResults);
    this.weather_timestamp = JdbcWritableBridge.readTimestamp(3, __dbResults);
    this.temperature_f = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.wind_chill_f = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.humidity_pct = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.pressure_in = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.visibility_mi = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.wind_direction = JdbcWritableBridge.readString(9, __dbResults);
    this.wind_speed_mph = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.precipitation_in = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.weather_condition = JdbcWritableBridge.readString(12, __dbResults);
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
    JdbcWritableBridge.writeLong(id, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(airport_code, 2 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeTimestamp(weather_timestamp, 3 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(temperature_f, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(wind_chill_f, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(humidity_pct, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(pressure_in, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(visibility_mi, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(wind_direction, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(wind_speed_mph, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(precipitation_in, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(weather_condition, 12 + __off, 12, __dbStmt);
    return 12;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(id, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(airport_code, 2 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeTimestamp(weather_timestamp, 3 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(temperature_f, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(wind_chill_f, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(humidity_pct, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(pressure_in, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(visibility_mi, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(wind_direction, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(wind_speed_mph, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(precipitation_in, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(weather_condition, 12 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.airport_code = null;
    } else {
    this.airport_code = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.weather_timestamp = null;
    } else {
    this.weather_timestamp = new Timestamp(__dataIn.readLong());
    this.weather_timestamp.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.temperature_f = null;
    } else {
    this.temperature_f = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.wind_chill_f = null;
    } else {
    this.wind_chill_f = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.humidity_pct = null;
    } else {
    this.humidity_pct = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.pressure_in = null;
    } else {
    this.pressure_in = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.visibility_mi = null;
    } else {
    this.visibility_mi = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.wind_direction = null;
    } else {
    this.wind_direction = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.wind_speed_mph = null;
    } else {
    this.wind_speed_mph = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.precipitation_in = null;
    } else {
    this.precipitation_in = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.weather_condition = null;
    } else {
    this.weather_condition = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.id);
    }
    if (null == this.airport_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, airport_code);
    }
    if (null == this.weather_timestamp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.weather_timestamp.getTime());
    __dataOut.writeInt(this.weather_timestamp.getNanos());
    }
    if (null == this.temperature_f) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.temperature_f, __dataOut);
    }
    if (null == this.wind_chill_f) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.wind_chill_f, __dataOut);
    }
    if (null == this.humidity_pct) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.humidity_pct, __dataOut);
    }
    if (null == this.pressure_in) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.pressure_in, __dataOut);
    }
    if (null == this.visibility_mi) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.visibility_mi, __dataOut);
    }
    if (null == this.wind_direction) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, wind_direction);
    }
    if (null == this.wind_speed_mph) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.wind_speed_mph, __dataOut);
    }
    if (null == this.precipitation_in) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.precipitation_in, __dataOut);
    }
    if (null == this.weather_condition) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, weather_condition);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.id);
    }
    if (null == this.airport_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, airport_code);
    }
    if (null == this.weather_timestamp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.weather_timestamp.getTime());
    __dataOut.writeInt(this.weather_timestamp.getNanos());
    }
    if (null == this.temperature_f) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.temperature_f, __dataOut);
    }
    if (null == this.wind_chill_f) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.wind_chill_f, __dataOut);
    }
    if (null == this.humidity_pct) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.humidity_pct, __dataOut);
    }
    if (null == this.pressure_in) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.pressure_in, __dataOut);
    }
    if (null == this.visibility_mi) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.visibility_mi, __dataOut);
    }
    if (null == this.wind_direction) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, wind_direction);
    }
    if (null == this.wind_speed_mph) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.wind_speed_mph, __dataOut);
    }
    if (null == this.precipitation_in) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.precipitation_in, __dataOut);
    }
    if (null == this.weather_condition) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, weather_condition);
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
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"\\N":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(airport_code==null?"\\N":airport_code, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(weather_timestamp==null?"\\N":"" + weather_timestamp, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(temperature_f==null?"\\N":temperature_f.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wind_chill_f==null?"\\N":wind_chill_f.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(humidity_pct==null?"\\N":humidity_pct.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(pressure_in==null?"\\N":pressure_in.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(visibility_mi==null?"\\N":visibility_mi.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wind_direction==null?"\\N":wind_direction, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wind_speed_mph==null?"\\N":wind_speed_mph.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(precipitation_in==null?"\\N":precipitation_in.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(weather_condition==null?"\\N":weather_condition, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"\\N":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(airport_code==null?"\\N":airport_code, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(weather_timestamp==null?"\\N":"" + weather_timestamp, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(temperature_f==null?"\\N":temperature_f.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wind_chill_f==null?"\\N":wind_chill_f.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(humidity_pct==null?"\\N":humidity_pct.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(pressure_in==null?"\\N":pressure_in.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(visibility_mi==null?"\\N":visibility_mi.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wind_direction==null?"\\N":wind_direction, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wind_speed_mph==null?"\\N":wind_speed_mph.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(precipitation_in==null?"\\N":precipitation_in.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(weather_condition==null?"\\N":weather_condition, delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.airport_code = null; } else {
      this.airport_code = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.weather_timestamp = null; } else {
      this.weather_timestamp = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.temperature_f = null; } else {
      this.temperature_f = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.wind_chill_f = null; } else {
      this.wind_chill_f = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.humidity_pct = null; } else {
      this.humidity_pct = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.pressure_in = null; } else {
      this.pressure_in = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.visibility_mi = null; } else {
      this.visibility_mi = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.wind_direction = null; } else {
      this.wind_direction = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.wind_speed_mph = null; } else {
      this.wind_speed_mph = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.precipitation_in = null; } else {
      this.precipitation_in = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.weather_condition = null; } else {
      this.weather_condition = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.airport_code = null; } else {
      this.airport_code = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.weather_timestamp = null; } else {
      this.weather_timestamp = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.temperature_f = null; } else {
      this.temperature_f = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.wind_chill_f = null; } else {
      this.wind_chill_f = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.humidity_pct = null; } else {
      this.humidity_pct = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.pressure_in = null; } else {
      this.pressure_in = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.visibility_mi = null; } else {
      this.visibility_mi = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.wind_direction = null; } else {
      this.wind_direction = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.wind_speed_mph = null; } else {
      this.wind_speed_mph = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.precipitation_in = null; } else {
      this.precipitation_in = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.weather_condition = null; } else {
      this.weather_condition = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    weather o = (weather) super.clone();
    o.weather_timestamp = (o.weather_timestamp != null) ? (java.sql.Timestamp) o.weather_timestamp.clone() : null;
    return o;
  }

  public void clone0(weather o) throws CloneNotSupportedException {
    o.weather_timestamp = (o.weather_timestamp != null) ? (java.sql.Timestamp) o.weather_timestamp.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("airport_code", this.airport_code);
    __sqoop$field_map.put("weather_timestamp", this.weather_timestamp);
    __sqoop$field_map.put("temperature_f", this.temperature_f);
    __sqoop$field_map.put("wind_chill_f", this.wind_chill_f);
    __sqoop$field_map.put("humidity_pct", this.humidity_pct);
    __sqoop$field_map.put("pressure_in", this.pressure_in);
    __sqoop$field_map.put("visibility_mi", this.visibility_mi);
    __sqoop$field_map.put("wind_direction", this.wind_direction);
    __sqoop$field_map.put("wind_speed_mph", this.wind_speed_mph);
    __sqoop$field_map.put("precipitation_in", this.precipitation_in);
    __sqoop$field_map.put("weather_condition", this.weather_condition);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("airport_code", this.airport_code);
    __sqoop$field_map.put("weather_timestamp", this.weather_timestamp);
    __sqoop$field_map.put("temperature_f", this.temperature_f);
    __sqoop$field_map.put("wind_chill_f", this.wind_chill_f);
    __sqoop$field_map.put("humidity_pct", this.humidity_pct);
    __sqoop$field_map.put("pressure_in", this.pressure_in);
    __sqoop$field_map.put("visibility_mi", this.visibility_mi);
    __sqoop$field_map.put("wind_direction", this.wind_direction);
    __sqoop$field_map.put("wind_speed_mph", this.wind_speed_mph);
    __sqoop$field_map.put("precipitation_in", this.precipitation_in);
    __sqoop$field_map.put("weather_condition", this.weather_condition);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
