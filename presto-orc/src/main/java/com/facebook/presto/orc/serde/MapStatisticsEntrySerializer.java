package com.facebook.presto.orc.serde;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.MapStatisticsEntry;
import com.facebook.presto.orc.proto.DwrfProto.KeyInfo;
import com.facebook.presto.orc.protobuf.CodedOutputStream;
import com.facebook.presto.orc.protobuf.InvalidProtocolBufferException;

public class MapStatisticsEntrySerializer extends Serializer<MapStatisticsEntry>{

  @Override
  public void write(Kryo kryo, Output output, MapStatisticsEntry object) {
    ColumnStatistics stats = object.getColumnStatistics();
    KeyInfo keyInfo = object.getKey();
    // serialize KeyInfo
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CodedOutputStream cos = CodedOutputStream.newInstance(baos);
    try {
      keyInfo.writeTo(cos);
      cos.flush();
    } catch (IOException e) {
      throw new RuntimeException (e);
    }
    byte[] buf = baos.toByteArray();
    output.writeInt(buf.length);
    output.writeBytes(buf);
    kryo.writeObject(output, stats);
  }

  @Override
  public MapStatisticsEntry read(Kryo kryo, Input input, Class<? extends MapStatisticsEntry> type) {
    int keySize = input.readInt();
    byte[] buf = input.readBytes(keySize);
    KeyInfo keyInfo = null;
    try {
      keyInfo = KeyInfo.parseFrom(buf);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException (e);
    }
    ColumnStatistics stats = kryo.readObject(input, ColumnStatistics.class);
    return new MapStatisticsEntry(keyInfo, stats);
  }

}
