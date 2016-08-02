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
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import net.openhft.chronicle.core.pool.StringBuilderPool;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * YCSB binding for <a href="http://chronicle.software/">Chronicle Engine</a>.
 *
 * See {@code chronicle/README.adoc} for details.
 */
public class ChronicleMapClient extends DB {

  public static final String[] NO_STRINGS = {};
  static final StringBuilderPool SBP = new StringBuilderPool();
  static int hit, miss;
  private Map<CharSequence, String> map;
  private Map<CharSequence, byte[]> mapValues;
  private Map<String, Map<String, byte[]>> cache;

  @NotNull
  private static Map<String, Map<String, byte[]>> getLinkedHashMap(final int size) {
    return Collections.synchronizedMap(new LinkedHashMap<String, Map<String, byte[]>>(size * 4 / 3, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, Map<String, byte[]>> eldest) {
        return size() >= size;
      }
    });
  }

  public void init() throws DBException {

    int recordCount = Integer.parseInt(getProperties().getProperty("recordcount", "1000000"));
    cache = getLinkedHashMap(recordCount / 10);
    try {
      map = acquireMap("maps/keys", recordCount, String.class);
      mapValues = acquireMap("maps/values", recordCount * 10, byte[].class);

    } catch (IOException ioe) {
      throw new DBException(ioe);
    }
  }

  private <T> ChronicleMap<CharSequence, T> acquireMap(String name, int recordCount, Class<T> valueClass) throws IOException {
    return ChronicleMapBuilder.of(CharSequence.class, valueClass)
      .entries(recordCount * 2 + 1000)
      .averageKeySize(40)
      .averageValueSize(128)
      .putReturnsNull(true)
      .removeReturnsNull(true)
      .createPersistedTo(new File(name + ".cm3"));
  }

  public void cleanup() throws DBException {
    System.out.println("cache hit:" + hit + " miss: " + miss);
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     HashMap<String, ByteIterator> result) {

    Map<String, byte[]> map0 = cache.get(key);
    if (map0 == null) {
      miss++;
      String keys0 = map.get(key);
      if (keys0 == null)
        return Status.ERROR;
      String[] keys = keys0.split(",");

      map0 = new LinkedHashMap<>(keys.length * 4 / 3);
      for (String s : keys) {
        if (fields == null || fields.contains(s))
          map0.put(s, mapValues.get(getKeyFor(key, s)));
      }
      cache.put(key, map0);
    } else {
      hit++;
    }
    for (Map.Entry<String, byte[]> entry : map0.entrySet())
      result.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue()));
    return Status.OK;
  }

  private CharSequence getKeyFor(String key, String s) {
    StringBuilder sb = SBP.acquireStringBuilder();
    return sb.append(key).append('|').append(s);
  }

  @Override
  public Status insert(String table, String key,
                       HashMap<String, ByteIterator> values) {
    cache.remove(key);
    StringBuilder sb = SBP.acquireStringBuilder();
    for (String k : values.keySet()) {
      sb.append(k).append(',');
    }
    sb.setLength(sb.length() - 1);
    ;
    map.put(key, sb.toString());
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      CharSequence keyFor = getKeyFor(key, entry.getKey());
      byte[] value = entry.getValue().toArray();
//      System.out.println("insert "+keyFor+" bytes:"+value.length);
      mapValues.put(keyFor, value);
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    cache.remove(key);
    String keys = map.remove(key);
    if (keys == null)
      return Status.ERROR;
    for (String s : keys.split(",")) {
      mapValues.remove(getKeyFor(key, s));
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key,
                       HashMap<String, ByteIterator> values) {
    Map<String, byte[]> map0 = cache.get(key);
    map0 = map0 == null ? null : new LinkedHashMap<>(map0);
    boolean newKey = false;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      CharSequence keyFor = getKeyFor(key, entry.getKey());
      newKey |= !mapValues.containsKey(keyFor);
      byte[] value = entry.getValue().toArray();
      mapValues.put(keyFor, value);
      if (map0 != null)
        map0.put(entry.getKey(), value);
    }
    if (map0 != null)
      cache.put(key, map0);

    return newKey ? Status.ERROR : Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.ERROR;
  }

}
