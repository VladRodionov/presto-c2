package com.facebook.presto.orc.serde;

import java.util.Optional;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.DwrfStripeCacheData;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;

import io.airlift.slice.Slice;

public class OrcFileTailSerializer extends Serializer<OrcFileTail> {

  @Override
  public void write(Kryo kryo, Output output, OrcFileTail object) {
    
    HiveWriterVersion version = object.getHiveWriterVersion();
    int bufferSize = object.getBufferSize();
    CompressionKind kind = object.getCompressionKind();
    Slice footerSlice = object.getFooterSlice();
    int footerSize = object.getFooterSize();
    Slice metaSlice = object.getMetadataSlice();
    int metaSize = object.getMetadataSize();
    Optional<DwrfStripeCacheData> dwrfStripeCacheData = object.getDwrfStripeCacheData();
    
    output.writeByte((byte)version.ordinal());
    output.writeInt(bufferSize);
    output.writeByte((byte)kind.ordinal());
    kryo.writeObject(output, footerSlice);
    output.writeInt(footerSize);
    kryo.writeObject(output, metaSlice);
    output.writeInt(metaSize);
    if (dwrfStripeCacheData.isPresent()) {
      output.writeByte(1);
      DwrfStripeCacheData data = dwrfStripeCacheData.get();
      kryo.writeObject(output, data);
    } else {
      output.writeByte(0);
    }
  }

  @Override
  public OrcFileTail read(Kryo kryo, Input input, Class<? extends OrcFileTail> type) {
    
    HiveWriterVersion version = HiveWriterVersion.values()[input.readByte()];
    int bufferSize = input.readInt();
    CompressionKind kind = CompressionKind.values()[input.readByte()];
    Slice footerSlice = kryo.readObject(input, Slice.class);
    int footerSize = input.readInt();
    Slice metaSlice = kryo.readObject(input, Slice.class);
    int metaSize = input.readInt();
    Optional<DwrfStripeCacheData> dwrfStripeCacheData = Optional.empty();
    boolean notNull = input.readByte() != 0;
    if (notNull) {
      DwrfStripeCacheData data = kryo.readObject(input, DwrfStripeCacheData.class);
      dwrfStripeCacheData = Optional.of(data);
    }
    return new OrcFileTail(version, bufferSize, kind, footerSlice, footerSize,
      metaSlice, metaSize, dwrfStripeCacheData);
  }

}
