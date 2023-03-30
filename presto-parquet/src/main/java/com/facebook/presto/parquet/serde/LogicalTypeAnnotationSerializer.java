/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.facebook.presto.parquet.serde;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntervalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapKeyValueTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

final public class LogicalTypeAnnotationSerializer extends Serializer<LogicalTypeAnnotation> {
  static enum LogicalTypeToken {
    MAP,
    LIST,
    STRING,
    MAP_KEY_VALUE,
    ENUM,
    DECIMAL,
    DATE,
    TIME,
    TIMESTAMP,
    INTEGER,
    JSON,
    BSON,
    UUID,
    INTERVAL;
  }
  
  @Override
  public void write(Kryo kryo, Output output, LogicalTypeAnnotation object) {
    if (object == null) {
      output.writeByte(0);
      return;
    } else {
      output.writeByte(1);
    }
    LogicalTypeToken type = getType(object);
    serialize(object, type, output);
  }

  @Override
  public LogicalTypeAnnotation read(Kryo kryo, Input input, Class<? extends LogicalTypeAnnotation> type) {
    int nullValue = input.readByte();
    if (nullValue == 0) {
      return null;
    }
    int ordinal = input.readInt();
    LogicalTypeToken type$ = LogicalTypeToken.values()[ordinal];
    return deserialize(type$, input);
  }
  
  private LogicalTypeToken getType(LogicalTypeAnnotation object) {
    if (object instanceof LogicalTypeAnnotation.BsonLogicalTypeAnnotation) {
      return LogicalTypeToken.BSON;
    } else if (object instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
      return LogicalTypeToken.DATE;
    } else if (object instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      return LogicalTypeToken.DECIMAL;
    } else if (object instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
      return LogicalTypeToken.ENUM;
    } else if (object instanceof LogicalTypeAnnotation.IntervalLogicalTypeAnnotation) {
      return LogicalTypeToken.INTERVAL;
    } else if (object instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
      return LogicalTypeToken.INTEGER;
    } else if (object instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation) {
      return LogicalTypeToken.JSON;
    } else if (object instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
      return LogicalTypeToken.LIST;
    } else if (object instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation) {
      return LogicalTypeToken.MAP_KEY_VALUE;
    } else if (object instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
      return LogicalTypeToken.MAP;
    } else if (object instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
      return LogicalTypeToken.STRING;
    } else if (object instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
      return LogicalTypeToken.TIME;
    } else if (object instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      return LogicalTypeToken.TIMESTAMP;
    } else if (object instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
      return LogicalTypeToken.UUID;
    } else {
      throw new RuntimeException("Unsupported type of LogicalTypeAnnotation " + object.getClass());
    }
  }
  
  private void serialize(LogicalTypeAnnotation object, LogicalTypeToken type, Output output) {
    output.writeInt(type.ordinal());
    switch (type) {
      case DECIMAL:
        DecimalLogicalTypeAnnotation decimal =
          (DecimalLogicalTypeAnnotation) object;
        int precision = decimal.getPrecision();
        int scale = decimal.getScale();
        output.writeInt(precision);
        output.writeInt(scale);
        break;
      case TIME:
        TimeLogicalTypeAnnotation time = 
          (TimeLogicalTypeAnnotation) object;
        TimeUnit unit = time.getUnit();
        boolean adj = time.isAdjustedToUTC();
        output.writeInt(unit.ordinal());
        output.writeBoolean(adj);
        break;
      case TIMESTAMP:
        TimestampLogicalTypeAnnotation timestamp = 
          (TimestampLogicalTypeAnnotation) object;
        unit = timestamp.getUnit();
        adj = timestamp.isAdjustedToUTC();
        output.writeInt(unit.ordinal());
        output.writeBoolean(adj);
        break;
      case INTEGER:
        IntLogicalTypeAnnotation integer = 
          (IntLogicalTypeAnnotation) object;
        int width = integer.getBitWidth();
        boolean isSigned = integer.isSigned();
        output.writeInt(width);
        output.writeBoolean(isSigned);
        break;
      default:  
    }
  }
  
  private LogicalTypeAnnotation deserialize(LogicalTypeToken type, Input input) {
    switch (type) {
      case BSON: return LogicalTypeAnnotation.bsonType();
      case DATE: return LogicalTypeAnnotation.dateType();
      case ENUM: return LogicalTypeAnnotation.enumType();
      case INTERVAL: return IntervalLogicalTypeAnnotation.getInstance();
      case JSON: return LogicalTypeAnnotation.jsonType();
      case LIST: return LogicalTypeAnnotation.listType();
      case MAP: return LogicalTypeAnnotation.mapType();
      case MAP_KEY_VALUE: return MapKeyValueTypeAnnotation.getInstance();
      case STRING: return LogicalTypeAnnotation.stringType();
      case UUID: return LogicalTypeAnnotation.uuidType();
      case DECIMAL: {
        int precision = input.readInt();
        int scale = input.readInt();
        return LogicalTypeAnnotation.decimalType(scale, precision);
      }
      case INTEGER:{
        int width = input.readInt();
        boolean isSigned = input.readBoolean();
        return LogicalTypeAnnotation.intType(width, isSigned);
      }
      case TIME: {
        int ordinal = input.readInt();
        boolean adj = input.readBoolean();
        return LogicalTypeAnnotation.timeType(adj, TimeUnit.values()[ordinal]);
      }
      case TIMESTAMP: {
        int ordinal = input.readInt();
        boolean adj = input.readBoolean();
        return LogicalTypeAnnotation.timestampType(adj, TimeUnit.values()[ordinal]);
      }
    }
    return null;
  }
  
}
