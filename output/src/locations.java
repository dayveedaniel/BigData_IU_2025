// ORM class for table 'locations'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sun May 18 16:32:11 MSK 2025
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

public class locations extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        locations.this.id = (Long)value;
      }
    });
    setters.put("start_lat", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        locations.this.start_lat = (java.math.BigDecimal)value;
      }
    });
    setters.put("start_lng", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        locations.this.start_lng = (java.math.BigDecimal)value;
      }
    });
    setters.put("end_lat", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        locations.this.end_lat = (java.math.BigDecimal)value;
      }
    });
    setters.put("end_lng", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        locations.this.end_lng = (java.math.BigDecimal)value;
      }
    });
    setters.put("street", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        locations.this.street = (String)value;
      }
    });
    setters.put("city", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        locations.this.city = (String)value;
      }
    });
    setters.put("county", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        locations.this.county = (String)value;
      }
    });
    setters.put("state", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        locations.this.state = (String)value;
      }
    });
    setters.put("zipcode", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        locations.this.zipcode = (String)value;
      }
    });
    setters.put("timezone", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        locations.this.timezone = (String)value;
      }
    });
  }
  public locations() {
    init0();
  }
  private Long id;
  public Long get_id() {
    return id;
  }
  public void set_id(Long id) {
    this.id = id;
  }
  public locations with_id(Long id) {
    this.id = id;
    return this;
  }
  private java.math.BigDecimal start_lat;
  public java.math.BigDecimal get_start_lat() {
    return start_lat;
  }
  public void set_start_lat(java.math.BigDecimal start_lat) {
    this.start_lat = start_lat;
  }
  public locations with_start_lat(java.math.BigDecimal start_lat) {
    this.start_lat = start_lat;
    return this;
  }
  private java.math.BigDecimal start_lng;
  public java.math.BigDecimal get_start_lng() {
    return start_lng;
  }
  public void set_start_lng(java.math.BigDecimal start_lng) {
    this.start_lng = start_lng;
  }
  public locations with_start_lng(java.math.BigDecimal start_lng) {
    this.start_lng = start_lng;
    return this;
  }
  private java.math.BigDecimal end_lat;
  public java.math.BigDecimal get_end_lat() {
    return end_lat;
  }
  public void set_end_lat(java.math.BigDecimal end_lat) {
    this.end_lat = end_lat;
  }
  public locations with_end_lat(java.math.BigDecimal end_lat) {
    this.end_lat = end_lat;
    return this;
  }
  private java.math.BigDecimal end_lng;
  public java.math.BigDecimal get_end_lng() {
    return end_lng;
  }
  public void set_end_lng(java.math.BigDecimal end_lng) {
    this.end_lng = end_lng;
  }
  public locations with_end_lng(java.math.BigDecimal end_lng) {
    this.end_lng = end_lng;
    return this;
  }
  private String street;
  public String get_street() {
    return street;
  }
  public void set_street(String street) {
    this.street = street;
  }
  public locations with_street(String street) {
    this.street = street;
    return this;
  }
  private String city;
  public String get_city() {
    return city;
  }
  public void set_city(String city) {
    this.city = city;
  }
  public locations with_city(String city) {
    this.city = city;
    return this;
  }
  private String county;
  public String get_county() {
    return county;
  }
  public void set_county(String county) {
    this.county = county;
  }
  public locations with_county(String county) {
    this.county = county;
    return this;
  }
  private String state;
  public String get_state() {
    return state;
  }
  public void set_state(String state) {
    this.state = state;
  }
  public locations with_state(String state) {
    this.state = state;
    return this;
  }
  private String zipcode;
  public String get_zipcode() {
    return zipcode;
  }
  public void set_zipcode(String zipcode) {
    this.zipcode = zipcode;
  }
  public locations with_zipcode(String zipcode) {
    this.zipcode = zipcode;
    return this;
  }
  private String timezone;
  public String get_timezone() {
    return timezone;
  }
  public void set_timezone(String timezone) {
    this.timezone = timezone;
  }
  public locations with_timezone(String timezone) {
    this.timezone = timezone;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof locations)) {
      return false;
    }
    locations that = (locations) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.start_lat == null ? that.start_lat == null : this.start_lat.equals(that.start_lat));
    equal = equal && (this.start_lng == null ? that.start_lng == null : this.start_lng.equals(that.start_lng));
    equal = equal && (this.end_lat == null ? that.end_lat == null : this.end_lat.equals(that.end_lat));
    equal = equal && (this.end_lng == null ? that.end_lng == null : this.end_lng.equals(that.end_lng));
    equal = equal && (this.street == null ? that.street == null : this.street.equals(that.street));
    equal = equal && (this.city == null ? that.city == null : this.city.equals(that.city));
    equal = equal && (this.county == null ? that.county == null : this.county.equals(that.county));
    equal = equal && (this.state == null ? that.state == null : this.state.equals(that.state));
    equal = equal && (this.zipcode == null ? that.zipcode == null : this.zipcode.equals(that.zipcode));
    equal = equal && (this.timezone == null ? that.timezone == null : this.timezone.equals(that.timezone));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof locations)) {
      return false;
    }
    locations that = (locations) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.start_lat == null ? that.start_lat == null : this.start_lat.equals(that.start_lat));
    equal = equal && (this.start_lng == null ? that.start_lng == null : this.start_lng.equals(that.start_lng));
    equal = equal && (this.end_lat == null ? that.end_lat == null : this.end_lat.equals(that.end_lat));
    equal = equal && (this.end_lng == null ? that.end_lng == null : this.end_lng.equals(that.end_lng));
    equal = equal && (this.street == null ? that.street == null : this.street.equals(that.street));
    equal = equal && (this.city == null ? that.city == null : this.city.equals(that.city));
    equal = equal && (this.county == null ? that.county == null : this.county.equals(that.county));
    equal = equal && (this.state == null ? that.state == null : this.state.equals(that.state));
    equal = equal && (this.zipcode == null ? that.zipcode == null : this.zipcode.equals(that.zipcode));
    equal = equal && (this.timezone == null ? that.timezone == null : this.timezone.equals(that.timezone));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readLong(1, __dbResults);
    this.start_lat = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.start_lng = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.end_lat = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.end_lng = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.street = JdbcWritableBridge.readString(6, __dbResults);
    this.city = JdbcWritableBridge.readString(7, __dbResults);
    this.county = JdbcWritableBridge.readString(8, __dbResults);
    this.state = JdbcWritableBridge.readString(9, __dbResults);
    this.zipcode = JdbcWritableBridge.readString(10, __dbResults);
    this.timezone = JdbcWritableBridge.readString(11, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.id = JdbcWritableBridge.readLong(1, __dbResults);
    this.start_lat = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.start_lng = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.end_lat = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.end_lng = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.street = JdbcWritableBridge.readString(6, __dbResults);
    this.city = JdbcWritableBridge.readString(7, __dbResults);
    this.county = JdbcWritableBridge.readString(8, __dbResults);
    this.state = JdbcWritableBridge.readString(9, __dbResults);
    this.zipcode = JdbcWritableBridge.readString(10, __dbResults);
    this.timezone = JdbcWritableBridge.readString(11, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(start_lat, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(start_lng, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(end_lat, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(end_lng, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(street, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(city, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(county, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(state, 9 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(zipcode, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(timezone, 11 + __off, 12, __dbStmt);
    return 11;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(id, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(start_lat, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(start_lng, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(end_lat, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(end_lng, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(street, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(city, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(county, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(state, 9 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(zipcode, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(timezone, 11 + __off, 12, __dbStmt);
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
        this.start_lat = null;
    } else {
    this.start_lat = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.start_lng = null;
    } else {
    this.start_lng = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.end_lat = null;
    } else {
    this.end_lat = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.end_lng = null;
    } else {
    this.end_lng = org.apache.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.street = null;
    } else {
    this.street = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.city = null;
    } else {
    this.city = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.county = null;
    } else {
    this.county = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.state = null;
    } else {
    this.state = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.zipcode = null;
    } else {
    this.zipcode = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.timezone = null;
    } else {
    this.timezone = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.id);
    }
    if (null == this.start_lat) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.start_lat, __dataOut);
    }
    if (null == this.start_lng) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.start_lng, __dataOut);
    }
    if (null == this.end_lat) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.end_lat, __dataOut);
    }
    if (null == this.end_lng) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.end_lng, __dataOut);
    }
    if (null == this.street) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, street);
    }
    if (null == this.city) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city);
    }
    if (null == this.county) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, county);
    }
    if (null == this.state) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, state);
    }
    if (null == this.zipcode) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, zipcode);
    }
    if (null == this.timezone) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, timezone);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.id);
    }
    if (null == this.start_lat) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.start_lat, __dataOut);
    }
    if (null == this.start_lng) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.start_lng, __dataOut);
    }
    if (null == this.end_lat) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.end_lat, __dataOut);
    }
    if (null == this.end_lng) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    org.apache.sqoop.lib.BigDecimalSerializer.write(this.end_lng, __dataOut);
    }
    if (null == this.street) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, street);
    }
    if (null == this.city) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city);
    }
    if (null == this.county) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, county);
    }
    if (null == this.state) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, state);
    }
    if (null == this.zipcode) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, zipcode);
    }
    if (null == this.timezone) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, timezone);
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
    __sb.append(FieldFormatter.escapeAndEnclose(start_lat==null?"\\N":start_lat.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_lng==null?"\\N":start_lng.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_lat==null?"\\N":end_lat.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_lng==null?"\\N":end_lng.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(street==null?"\\N":street, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(city==null?"\\N":city, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(county==null?"\\N":county, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(state==null?"\\N":state, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(zipcode==null?"\\N":zipcode, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(timezone==null?"\\N":timezone, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"\\N":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_lat==null?"\\N":start_lat.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_lng==null?"\\N":start_lng.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_lat==null?"\\N":end_lat.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_lng==null?"\\N":end_lng.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(street==null?"\\N":street, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(city==null?"\\N":city, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(county==null?"\\N":county, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(state==null?"\\N":state, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(zipcode==null?"\\N":zipcode, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(timezone==null?"\\N":timezone, delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.start_lat = null; } else {
      this.start_lat = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.start_lng = null; } else {
      this.start_lng = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.end_lat = null; } else {
      this.end_lat = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.end_lng = null; } else {
      this.end_lng = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.street = null; } else {
      this.street = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.city = null; } else {
      this.city = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.county = null; } else {
      this.county = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.state = null; } else {
      this.state = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.zipcode = null; } else {
      this.zipcode = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.timezone = null; } else {
      this.timezone = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.start_lat = null; } else {
      this.start_lat = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.start_lng = null; } else {
      this.start_lng = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.end_lat = null; } else {
      this.end_lat = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.end_lng = null; } else {
      this.end_lng = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.street = null; } else {
      this.street = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.city = null; } else {
      this.city = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.county = null; } else {
      this.county = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.state = null; } else {
      this.state = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.zipcode = null; } else {
      this.zipcode = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.timezone = null; } else {
      this.timezone = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    locations o = (locations) super.clone();
    return o;
  }

  public void clone0(locations o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("start_lat", this.start_lat);
    __sqoop$field_map.put("start_lng", this.start_lng);
    __sqoop$field_map.put("end_lat", this.end_lat);
    __sqoop$field_map.put("end_lng", this.end_lng);
    __sqoop$field_map.put("street", this.street);
    __sqoop$field_map.put("city", this.city);
    __sqoop$field_map.put("county", this.county);
    __sqoop$field_map.put("state", this.state);
    __sqoop$field_map.put("zipcode", this.zipcode);
    __sqoop$field_map.put("timezone", this.timezone);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("start_lat", this.start_lat);
    __sqoop$field_map.put("start_lng", this.start_lng);
    __sqoop$field_map.put("end_lat", this.end_lat);
    __sqoop$field_map.put("end_lng", this.end_lng);
    __sqoop$field_map.put("street", this.street);
    __sqoop$field_map.put("city", this.city);
    __sqoop$field_map.put("county", this.county);
    __sqoop$field_map.put("state", this.state);
    __sqoop$field_map.put("zipcode", this.zipcode);
    __sqoop$field_map.put("timezone", this.timezone);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
