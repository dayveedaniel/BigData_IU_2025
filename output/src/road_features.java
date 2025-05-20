// ORM class for table 'road_features'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sun May 18 16:37:33 MSK 2025
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

public class road_features extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        road_features.this.id = (Integer)value;
      }
    });
    setters.put("amenity", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        road_features.this.amenity = (Boolean)value;
      }
    });
    setters.put("bump", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        road_features.this.bump = (Boolean)value;
      }
    });
    setters.put("crossing", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        road_features.this.crossing = (Boolean)value;
      }
    });
    setters.put("give_way", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        road_features.this.give_way = (Boolean)value;
      }
    });
    setters.put("junction", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        road_features.this.junction = (Boolean)value;
      }
    });
    setters.put("no_exit", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        road_features.this.no_exit = (Boolean)value;
      }
    });
    setters.put("railway", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        road_features.this.railway = (Boolean)value;
      }
    });
    setters.put("roundabout", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        road_features.this.roundabout = (Boolean)value;
      }
    });
    setters.put("station", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        road_features.this.station = (Boolean)value;
      }
    });
    setters.put("stop", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        road_features.this.stop = (Boolean)value;
      }
    });
    setters.put("traffic_calming", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        road_features.this.traffic_calming = (Boolean)value;
      }
    });
    setters.put("traffic_signal", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        road_features.this.traffic_signal = (Boolean)value;
      }
    });
  }
  public road_features() {
    init0();
  }
  private Integer id;
  public Integer get_id() {
    return id;
  }
  public void set_id(Integer id) {
    this.id = id;
  }
  public road_features with_id(Integer id) {
    this.id = id;
    return this;
  }
  private Boolean amenity;
  public Boolean get_amenity() {
    return amenity;
  }
  public void set_amenity(Boolean amenity) {
    this.amenity = amenity;
  }
  public road_features with_amenity(Boolean amenity) {
    this.amenity = amenity;
    return this;
  }
  private Boolean bump;
  public Boolean get_bump() {
    return bump;
  }
  public void set_bump(Boolean bump) {
    this.bump = bump;
  }
  public road_features with_bump(Boolean bump) {
    this.bump = bump;
    return this;
  }
  private Boolean crossing;
  public Boolean get_crossing() {
    return crossing;
  }
  public void set_crossing(Boolean crossing) {
    this.crossing = crossing;
  }
  public road_features with_crossing(Boolean crossing) {
    this.crossing = crossing;
    return this;
  }
  private Boolean give_way;
  public Boolean get_give_way() {
    return give_way;
  }
  public void set_give_way(Boolean give_way) {
    this.give_way = give_way;
  }
  public road_features with_give_way(Boolean give_way) {
    this.give_way = give_way;
    return this;
  }
  private Boolean junction;
  public Boolean get_junction() {
    return junction;
  }
  public void set_junction(Boolean junction) {
    this.junction = junction;
  }
  public road_features with_junction(Boolean junction) {
    this.junction = junction;
    return this;
  }
  private Boolean no_exit;
  public Boolean get_no_exit() {
    return no_exit;
  }
  public void set_no_exit(Boolean no_exit) {
    this.no_exit = no_exit;
  }
  public road_features with_no_exit(Boolean no_exit) {
    this.no_exit = no_exit;
    return this;
  }
  private Boolean railway;
  public Boolean get_railway() {
    return railway;
  }
  public void set_railway(Boolean railway) {
    this.railway = railway;
  }
  public road_features with_railway(Boolean railway) {
    this.railway = railway;
    return this;
  }
  private Boolean roundabout;
  public Boolean get_roundabout() {
    return roundabout;
  }
  public void set_roundabout(Boolean roundabout) {
    this.roundabout = roundabout;
  }
  public road_features with_roundabout(Boolean roundabout) {
    this.roundabout = roundabout;
    return this;
  }
  private Boolean station;
  public Boolean get_station() {
    return station;
  }
  public void set_station(Boolean station) {
    this.station = station;
  }
  public road_features with_station(Boolean station) {
    this.station = station;
    return this;
  }
  private Boolean stop;
  public Boolean get_stop() {
    return stop;
  }
  public void set_stop(Boolean stop) {
    this.stop = stop;
  }
  public road_features with_stop(Boolean stop) {
    this.stop = stop;
    return this;
  }
  private Boolean traffic_calming;
  public Boolean get_traffic_calming() {
    return traffic_calming;
  }
  public void set_traffic_calming(Boolean traffic_calming) {
    this.traffic_calming = traffic_calming;
  }
  public road_features with_traffic_calming(Boolean traffic_calming) {
    this.traffic_calming = traffic_calming;
    return this;
  }
  private Boolean traffic_signal;
  public Boolean get_traffic_signal() {
    return traffic_signal;
  }
  public void set_traffic_signal(Boolean traffic_signal) {
    this.traffic_signal = traffic_signal;
  }
  public road_features with_traffic_signal(Boolean traffic_signal) {
    this.traffic_signal = traffic_signal;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof road_features)) {
      return false;
    }
    road_features that = (road_features) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.amenity == null ? that.amenity == null : this.amenity.equals(that.amenity));
    equal = equal && (this.bump == null ? that.bump == null : this.bump.equals(that.bump));
    equal = equal && (this.crossing == null ? that.crossing == null : this.crossing.equals(that.crossing));
    equal = equal && (this.give_way == null ? that.give_way == null : this.give_way.equals(that.give_way));
    equal = equal && (this.junction == null ? that.junction == null : this.junction.equals(that.junction));
    equal = equal && (this.no_exit == null ? that.no_exit == null : this.no_exit.equals(that.no_exit));
    equal = equal && (this.railway == null ? that.railway == null : this.railway.equals(that.railway));
    equal = equal && (this.roundabout == null ? that.roundabout == null : this.roundabout.equals(that.roundabout));
    equal = equal && (this.station == null ? that.station == null : this.station.equals(that.station));
    equal = equal && (this.stop == null ? that.stop == null : this.stop.equals(that.stop));
    equal = equal && (this.traffic_calming == null ? that.traffic_calming == null : this.traffic_calming.equals(that.traffic_calming));
    equal = equal && (this.traffic_signal == null ? that.traffic_signal == null : this.traffic_signal.equals(that.traffic_signal));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof road_features)) {
      return false;
    }
    road_features that = (road_features) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.amenity == null ? that.amenity == null : this.amenity.equals(that.amenity));
    equal = equal && (this.bump == null ? that.bump == null : this.bump.equals(that.bump));
    equal = equal && (this.crossing == null ? that.crossing == null : this.crossing.equals(that.crossing));
    equal = equal && (this.give_way == null ? that.give_way == null : this.give_way.equals(that.give_way));
    equal = equal && (this.junction == null ? that.junction == null : this.junction.equals(that.junction));
    equal = equal && (this.no_exit == null ? that.no_exit == null : this.no_exit.equals(that.no_exit));
    equal = equal && (this.railway == null ? that.railway == null : this.railway.equals(that.railway));
    equal = equal && (this.roundabout == null ? that.roundabout == null : this.roundabout.equals(that.roundabout));
    equal = equal && (this.station == null ? that.station == null : this.station.equals(that.station));
    equal = equal && (this.stop == null ? that.stop == null : this.stop.equals(that.stop));
    equal = equal && (this.traffic_calming == null ? that.traffic_calming == null : this.traffic_calming.equals(that.traffic_calming));
    equal = equal && (this.traffic_signal == null ? that.traffic_signal == null : this.traffic_signal.equals(that.traffic_signal));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.amenity = JdbcWritableBridge.readBoolean(2, __dbResults);
    this.bump = JdbcWritableBridge.readBoolean(3, __dbResults);
    this.crossing = JdbcWritableBridge.readBoolean(4, __dbResults);
    this.give_way = JdbcWritableBridge.readBoolean(5, __dbResults);
    this.junction = JdbcWritableBridge.readBoolean(6, __dbResults);
    this.no_exit = JdbcWritableBridge.readBoolean(7, __dbResults);
    this.railway = JdbcWritableBridge.readBoolean(8, __dbResults);
    this.roundabout = JdbcWritableBridge.readBoolean(9, __dbResults);
    this.station = JdbcWritableBridge.readBoolean(10, __dbResults);
    this.stop = JdbcWritableBridge.readBoolean(11, __dbResults);
    this.traffic_calming = JdbcWritableBridge.readBoolean(12, __dbResults);
    this.traffic_signal = JdbcWritableBridge.readBoolean(13, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.amenity = JdbcWritableBridge.readBoolean(2, __dbResults);
    this.bump = JdbcWritableBridge.readBoolean(3, __dbResults);
    this.crossing = JdbcWritableBridge.readBoolean(4, __dbResults);
    this.give_way = JdbcWritableBridge.readBoolean(5, __dbResults);
    this.junction = JdbcWritableBridge.readBoolean(6, __dbResults);
    this.no_exit = JdbcWritableBridge.readBoolean(7, __dbResults);
    this.railway = JdbcWritableBridge.readBoolean(8, __dbResults);
    this.roundabout = JdbcWritableBridge.readBoolean(9, __dbResults);
    this.station = JdbcWritableBridge.readBoolean(10, __dbResults);
    this.stop = JdbcWritableBridge.readBoolean(11, __dbResults);
    this.traffic_calming = JdbcWritableBridge.readBoolean(12, __dbResults);
    this.traffic_signal = JdbcWritableBridge.readBoolean(13, __dbResults);
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
    JdbcWritableBridge.writeBoolean(amenity, 2 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(bump, 3 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(crossing, 4 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(give_way, 5 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(junction, 6 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(no_exit, 7 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(railway, 8 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(roundabout, 9 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(station, 10 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(stop, 11 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(traffic_calming, 12 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(traffic_signal, 13 + __off, -7, __dbStmt);
    return 13;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(id, 1 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeBoolean(amenity, 2 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(bump, 3 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(crossing, 4 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(give_way, 5 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(junction, 6 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(no_exit, 7 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(railway, 8 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(roundabout, 9 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(station, 10 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(stop, 11 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(traffic_calming, 12 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(traffic_signal, 13 + __off, -7, __dbStmt);
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
        this.amenity = null;
    } else {
    this.amenity = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.bump = null;
    } else {
    this.bump = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.crossing = null;
    } else {
    this.crossing = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.give_way = null;
    } else {
    this.give_way = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.junction = null;
    } else {
    this.junction = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.no_exit = null;
    } else {
    this.no_exit = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.railway = null;
    } else {
    this.railway = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.roundabout = null;
    } else {
    this.roundabout = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.station = null;
    } else {
    this.station = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.stop = null;
    } else {
    this.stop = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.traffic_calming = null;
    } else {
    this.traffic_calming = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.traffic_signal = null;
    } else {
    this.traffic_signal = Boolean.valueOf(__dataIn.readBoolean());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.amenity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.amenity);
    }
    if (null == this.bump) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.bump);
    }
    if (null == this.crossing) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.crossing);
    }
    if (null == this.give_way) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.give_way);
    }
    if (null == this.junction) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.junction);
    }
    if (null == this.no_exit) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.no_exit);
    }
    if (null == this.railway) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.railway);
    }
    if (null == this.roundabout) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.roundabout);
    }
    if (null == this.station) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.station);
    }
    if (null == this.stop) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.stop);
    }
    if (null == this.traffic_calming) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.traffic_calming);
    }
    if (null == this.traffic_signal) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.traffic_signal);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.amenity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.amenity);
    }
    if (null == this.bump) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.bump);
    }
    if (null == this.crossing) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.crossing);
    }
    if (null == this.give_way) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.give_way);
    }
    if (null == this.junction) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.junction);
    }
    if (null == this.no_exit) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.no_exit);
    }
    if (null == this.railway) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.railway);
    }
    if (null == this.roundabout) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.roundabout);
    }
    if (null == this.station) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.station);
    }
    if (null == this.stop) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.stop);
    }
    if (null == this.traffic_calming) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.traffic_calming);
    }
    if (null == this.traffic_signal) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.traffic_signal);
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
    __sb.append(FieldFormatter.escapeAndEnclose(amenity==null?"\\N":"" + amenity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bump==null?"\\N":"" + bump, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(crossing==null?"\\N":"" + crossing, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(give_way==null?"\\N":"" + give_way, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(junction==null?"\\N":"" + junction, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(no_exit==null?"\\N":"" + no_exit, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(railway==null?"\\N":"" + railway, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(roundabout==null?"\\N":"" + roundabout, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(station==null?"\\N":"" + station, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(stop==null?"\\N":"" + stop, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(traffic_calming==null?"\\N":"" + traffic_calming, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(traffic_signal==null?"\\N":"" + traffic_signal, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"\\N":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(amenity==null?"\\N":"" + amenity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bump==null?"\\N":"" + bump, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(crossing==null?"\\N":"" + crossing, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(give_way==null?"\\N":"" + give_way, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(junction==null?"\\N":"" + junction, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(no_exit==null?"\\N":"" + no_exit, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(railway==null?"\\N":"" + railway, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(roundabout==null?"\\N":"" + roundabout, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(station==null?"\\N":"" + station, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(stop==null?"\\N":"" + stop, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(traffic_calming==null?"\\N":"" + traffic_calming, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(traffic_signal==null?"\\N":"" + traffic_signal, delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.amenity = null; } else {
      this.amenity = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.bump = null; } else {
      this.bump = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.crossing = null; } else {
      this.crossing = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.give_way = null; } else {
      this.give_way = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.junction = null; } else {
      this.junction = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.no_exit = null; } else {
      this.no_exit = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.railway = null; } else {
      this.railway = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.roundabout = null; } else {
      this.roundabout = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.station = null; } else {
      this.station = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.stop = null; } else {
      this.stop = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.traffic_calming = null; } else {
      this.traffic_calming = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.traffic_signal = null; } else {
      this.traffic_signal = BooleanParser.valueOf(__cur_str);
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.amenity = null; } else {
      this.amenity = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.bump = null; } else {
      this.bump = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.crossing = null; } else {
      this.crossing = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.give_way = null; } else {
      this.give_way = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.junction = null; } else {
      this.junction = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.no_exit = null; } else {
      this.no_exit = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.railway = null; } else {
      this.railway = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.roundabout = null; } else {
      this.roundabout = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.station = null; } else {
      this.station = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.stop = null; } else {
      this.stop = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.traffic_calming = null; } else {
      this.traffic_calming = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.traffic_signal = null; } else {
      this.traffic_signal = BooleanParser.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    road_features o = (road_features) super.clone();
    return o;
  }

  public void clone0(road_features o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("amenity", this.amenity);
    __sqoop$field_map.put("bump", this.bump);
    __sqoop$field_map.put("crossing", this.crossing);
    __sqoop$field_map.put("give_way", this.give_way);
    __sqoop$field_map.put("junction", this.junction);
    __sqoop$field_map.put("no_exit", this.no_exit);
    __sqoop$field_map.put("railway", this.railway);
    __sqoop$field_map.put("roundabout", this.roundabout);
    __sqoop$field_map.put("station", this.station);
    __sqoop$field_map.put("stop", this.stop);
    __sqoop$field_map.put("traffic_calming", this.traffic_calming);
    __sqoop$field_map.put("traffic_signal", this.traffic_signal);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("amenity", this.amenity);
    __sqoop$field_map.put("bump", this.bump);
    __sqoop$field_map.put("crossing", this.crossing);
    __sqoop$field_map.put("give_way", this.give_way);
    __sqoop$field_map.put("junction", this.junction);
    __sqoop$field_map.put("no_exit", this.no_exit);
    __sqoop$field_map.put("railway", this.railway);
    __sqoop$field_map.put("roundabout", this.roundabout);
    __sqoop$field_map.put("station", this.station);
    __sqoop$field_map.put("stop", this.stop);
    __sqoop$field_map.put("traffic_calming", this.traffic_calming);
    __sqoop$field_map.put("traffic_signal", this.traffic_signal);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
