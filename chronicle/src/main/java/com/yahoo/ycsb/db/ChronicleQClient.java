/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.core.pool.StringBuilderPool;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.yahoo.ycsb.db.EncodeStringUtils.*;

/**
 * YCSB binding for <a href="http://chronicle.software/">Chronicle Engine</a>.
 *
 * See {@code chronicle/README.adoc} for details.
 */
public class ChronicleQClient extends DB {
  private Map<CharSequence, Long> map;
  private ChronicleQueue queue;

  public void init() throws DBException {

    int recordCount = Integer.parseInt(getProperties().getProperty("recordcount", "1000000"));

    try {
      map = acquireMap("maps/index", recordCount);
      queue = acquireQueue("maps/queue");

    } catch (IOException ioe) {
      throw new DBException(ioe);
    }
  }

  private ChronicleQueue acquireQueue(String name) {
    return SingleChronicleQueueBuilder.binary(name).build();
  }

  private ChronicleMap<CharSequence, Long> acquireMap(String name, int recordCount) throws IOException {
    return ChronicleMapBuilder.of(CharSequence.class, Long.class)
      .entries(recordCount * 2 + 1000)
      .averageKeySize(32)
      .putReturnsNull(true)
      .removeReturnsNull(true)
      .createPersistedTo(new File(name + ".cm3"));
  }

  public void cleanup() throws DBException {
  }

  private final ThreadLocal<ExcerptTailer> tailerTL = new ThreadLocal<>();

  private ExcerptTailer acquireTailer() {
    ExcerptTailer tailer = tailerTL.get();
    if (tailer == null) {
      tailerTL.set(tailer = queue.createTailer());
      tailer.direction(TailerDirection.NONE);
    }
    return tailer;
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     HashMap<String, ByteIterator> result) {

    Long index = map.get(key);
    if (index == null)
      return Status.NOT_FOUND;

    ExcerptTailer tailer = acquireTailer();
    if (!tailer.moveToIndex(index))
      return Status.ERROR;

    try (DocumentContext dc = tailer.readingDocument()) {
      Wire wire = dc.wire();
      while (wire.hasMore()) {
        String key0 = wire.readEvent(String.class);
        byte[] value = decode(wire.getValueIn().bytes());
        result.put(key0, new ByteArrayByteIterator(value));
      }
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
                       HashMap<String, ByteIterator> values) {

    Wire wire = getWire();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String key0 = entry.getKey();
      byte[] value = entry.getValue().toArray();
      wire.writeEventName(key0).bytes(encode(value));
    }

    long index;
    try (DocumentContext dca = acquireAppender().writingDocument()) {
      dca.wire().bytes().write(wire.bytes());
      index = dca.index();
    }
    map.put(key, index);
    return Status.OK;
  }

  @NotNull
  private ExcerptAppender acquireAppender() {
    ExcerptAppender appender = queue.acquireAppender();
    appender.padToCacheAlign(true);
    return appender;
  }

  @Override
  public Status delete(String table, String key) {
    Long index = map.remove(key);
    if (index == null)
      return Status.ERROR;
    return Status.OK;
  }

  static final ThreadLocal<Wire> WIRE_TL = new ThreadLocal<>();

  public static Wire getWire() {
    Wire wire = WIRE_TL.get();
    if (wire == null)
      WIRE_TL.set(wire = new BinaryWire(NativeBytesStore.nativeStore(1024).bytesForWrite()));
    else
      wire.clear();
    return wire;
  }

  @Override
  public Status update(String table, String key,
                       HashMap<String, ByteIterator> values) {
    Long index = map.get(key);
    if (index == null)
      return Status.NOT_FOUND;

    ExcerptTailer tailer = acquireTailer();
    if (!tailer.moveToIndex(index)) {
      System.err.println("Failed to move to " + Long.toHexString(index));
      return Status.ERROR;
    }

    Wire out = getWire();
    try (DocumentContext dct = tailer.readingDocument()) {
      Wire wire = dct.wire();
      while (wire.hasMore()) {
        String key0 = wire.readEvent(String.class);
        byte[] value = wire.getValueIn().bytes();
        ByteIterator remove = values.remove(key0);
        if (remove != null) {
          value = encode(remove.toArray());
        }
        out.writeEventName(key0).bytes(value);
      }
      if (!values.isEmpty())
        throw new AssertionError();
    }
    long index2;
    try (DocumentContext dca = acquireAppender().writingDocument()) {
      dca.wire().bytes().write(out.bytes());
      index2 = dca.index();
    }
    map.put(key, index2);
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.ERROR;
  }

}
