// ORM class for table 'twilight'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sun May 18 16:37:22 MSK 2025
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

public class twilight extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        twilight.this.id = (Integer)value;
      }
    });
    setters.put("sunrise_sunset", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        twilight.this.sunrise_sunset = (String)value;
      }
    });
    setters.put("civil_twilight", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        twilight.this.civil_twilight = (String)value;
      }
    });
    setters.put("nautical_twilight", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        twilight.this.nautical_twilight = (String)value;
      }
    });
    setters.put("astronomical_twilight", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        twilight.this.astronomical_twilight = (String)value;
      }
    });
  }
  public twilight() {
    init0();
  }
  private Integer id;
  public Integer get_id() {
    return id;
  }
  public void set_id(Integer id) {
    this.id = id;
  }
  public twilight with_id(Integer id) {
    this.id = id;
    return this;
  }
  private String sunrise_sunset;
  public String get_sunrise_sunset() {
    return sunrise_sunset;
  }
  public void set_sunrise_sunset(String sunrise_sunset) {
    this.sunrise_sunset = sunrise_sunset;
  }
  public twilight with_sunrise_sunset(String sunrise_sunset) {
    this.sunrise_sunset = sunrise_sunset;
    return this;
  }
  private String civil_twilight;
  public String get_civil_twilight() {
    return civil_twilight;
  }
  public void set_civil_twilight(String civil_twilight) {
    this.civil_twilight = civil_twilight;
  }
  public twilight with_civil_twilight(String civil_twilight) {
    this.civil_twilight = civil_twilight;
    return this;
  }
  private String nautical_twilight;
  public String get_nautical_twilight() {
    return nautical_twilight;
  }
  public void set_nautical_twilight(String nautical_twilight) {
    this.nautical_twilight = nautical_twilight;
  }
  public twilight with_nautical_twilight(String nautical_twilight) {
    this.nautical_twilight = nautical_twilight;
    return this;
  }
  private String astronomical_twilight;
  public String get_astronomical_twilight() {
    return astronomical_twilight;
  }
  public void set_astronomical_twilight(String astronomical_twilight) {
    this.astronomical_twilight = astronomical_twilight;
  }
  public twilight with_astronomical_twilight(String astronomical_twilight) {
    this.astronomical_twilight = astronomical_twilight;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof twilight)) {
      return false;
    }
    twilight that = (twilight) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.sunrise_sunset == null ? that.sunrise_sunset == null : this.sunrise_sunset.equals(that.sunrise_sunset));
    equal = equal && (this.civil_twilight == null ? that.civil_twilight == null : this.civil_twilight.equals(that.civil_twilight));
    equal = equal && (this.nautical_twilight == null ? that.nautical_twilight == null : this.nautical_twilight.equals(that.nautical_twilight));
    equal = equal && (this.astronomical_twilight == null ? that.astronomical_twilight == null : this.astronomical_twilight.equals(that.astronomical_twilight));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof twilight)) {
      return false;
    }
    twilight that = (twilight) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.sunrise_sunset == null ? that.sunrise_sunset == null : this.sunrise_sunset.equals(that.sunrise_sunset));
    equal = equal && (this.civil_twilight == null ? that.civil_twilight == null : this.civil_twilight.equals(that.civil_twilight));
    equal = equal && (this.nautical_twilight == null ? that.nautical_twilight == null : this.nautical_twilight.equals(that.nautical_twilight));
    equal = equal && (this.astronomical_twilight == null ? that.astronomical_twilight == null : this.astronomical_twilight.equals(that.astronomical_twilight));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.sunrise_sunset = JdbcWritableBridge.readString(2, __dbResults);
    this.civil_twilight = JdbcWritableBridge.readString(3, __dbResults);
    this.nautical_twilight = JdbcWritableBridge.readString(4, __dbResults);
    this.astronomical_twilight = JdbcWritableBridge.readString(5, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.sunrise_sunset = JdbcWritableBridge.readString(2, __dbResults);
    this.civil_twilight = JdbcWritableBridge.readString(3, __dbResults);
    this.nautical_twilight = JdbcWritableBridge.readString(4, __dbResults);
    this.astronomical_twilight = JdbcWritableBridge.readString(5, __dbResults);
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
    JdbcWritableBridge.writeInteger(id, 1 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeString(sunrise_sunset, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(civil_twilight, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(nautical_twilight, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(astronomical_twilight, 5 + __off, 12, __dbStmt);
    return 5;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(id, 1 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeString(sunrise_sunset, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(civil_twilight, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(nautical_twilight, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(astronomical_twilight, 5 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.sunrise_sunset = null;
    } else {
    this.sunrise_sunset = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.civil_twilight = null;
    } else {
    this.civil_twilight = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.nautical_twilight = null;
    } else {
    this.nautical_twilight = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.astronomical_twilight = null;
    } else {
    this.astronomical_twilight = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.sunrise_sunset) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, sunrise_sunset);
    }
    if (null == this.civil_twilight) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, civil_twilight);
    }
    if (null == this.nautical_twilight) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, nautical_twilight);
    }
    if (null == this.astronomical_twilight) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, astronomical_twilight);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.sunrise_sunset) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, sunrise_sunset);
    }
    if (null == this.civil_twilight) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, civil_twilight);
    }
    if (null == this.nautical_twilight) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, nautical_twilight);
    }
    if (null == this.astronomical_twilight) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, astronomical_twilight);
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
    __sb.append(FieldFormatter.escapeAndEnclose(sunrise_sunset==null?"\\N":sunrise_sunset, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(civil_twilight==null?"\\N":civil_twilight, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(nautical_twilight==null?"\\N":nautical_twilight, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(astronomical_twilight==null?"\\N":astronomical_twilight, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"\\N":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sunrise_sunset==null?"\\N":sunrise_sunset, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(civil_twilight==null?"\\N":civil_twilight, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(nautical_twilight==null?"\\N":nautical_twilight, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(astronomical_twilight==null?"\\N":astronomical_twilight, delimiters));
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
      this.id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.sunrise_sunset = null; } else {
      this.sunrise_sunset = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.civil_twilight = null; } else {
      this.civil_twilight = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.nautical_twilight = null; } else {
      this.nautical_twilight = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.astronomical_twilight = null; } else {
      this.astronomical_twilight = __cur_str;
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
      this.id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.sunrise_sunset = null; } else {
      this.sunrise_sunset = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.civil_twilight = null; } else {
      this.civil_twilight = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.nautical_twilight = null; } else {
      this.nautical_twilight = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.astronomical_twilight = null; } else {
      this.astronomical_twilight = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    twilight o = (twilight) super.clone();
    return o;
  }

  public void clone0(twilight o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("sunrise_sunset", this.sunrise_sunset);
    __sqoop$field_map.put("civil_twilight", this.civil_twilight);
    __sqoop$field_map.put("nautical_twilight", this.nautical_twilight);
    __sqoop$field_map.put("astronomical_twilight", this.astronomical_twilight);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("sunrise_sunset", this.sunrise_sunset);
    __sqoop$field_map.put("civil_twilight", this.civil_twilight);
    __sqoop$field_map.put("nautical_twilight", this.nautical_twilight);
    __sqoop$field_map.put("astronomical_twilight", this.astronomical_twilight);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
