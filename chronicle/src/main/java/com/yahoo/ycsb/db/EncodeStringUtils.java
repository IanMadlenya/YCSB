package com.yahoo.ycsb.db;

import net.openhft.chronicle.core.UnsafeMemory;
import sun.misc.Unsafe;

/**
 * Created by peter on 31/07/16.
 */
public enum EncodeStringUtils {
  ; // no instances

  public static byte[] encode(byte[] bytes) {
    byte[] ret = new byte[(bytes.length + 5) / 6 * 4];
    Unsafe unsafe = UnsafeMemory.UNSAFE;
    for (int i = 0; i < ret.length / 4; i++) {
      int n = getInt(bytes, i * 6);
      unsafe.putInt(ret, Unsafe.ARRAY_BYTE_BASE_OFFSET + i * 4L, n);
    }
    return ret;
  }

  public static byte[] decode(byte[] bytes) {
    byte[] ret = new byte[100];
    Unsafe unsafe = UnsafeMemory.UNSAFE;
    for (int i = 0; i < bytes.length / 4; i++) {
      int n = unsafe.getInt(ret, Unsafe.ARRAY_BYTE_BASE_OFFSET + i * 4L);
      getBytes(ret, i * 6, n);
    }
    return ret;
  }

  public static int getInt(byte[] bytes, int offset) {
    int n = 0;
    switch (bytes.length - offset) {
      default:
        n = bytes[offset + 5] - ' ';
        n <<= 5;
      case 5:
        n |= ((bytes[offset + 4] - ' ') & 31);
        n <<= 5;
      case 4:
        n |= ((bytes[offset + 3] - ' ') & 31);
        n <<= 5;
      case 3:
        n |= ((bytes[offset + 2] - ' ') & 31);
        n <<= 5;
      case 2:
        n |= ((bytes[offset + 1] - ' ') & 31);
        n <<= 5;
      case 1:
        n |= ((bytes[offset + 0] - ' ') & 31);
      case 0:
        break;
    }
    return n;
  }

  public static void getBytes(byte[] buffer, int base, int n) {
    switch (buffer.length - base) {
      default:
        buffer[base + 5] = (byte) (((n >> 25) & 95) + ' ');
      case 5:
        buffer[base + 4] = (byte) (((n >> 20) & 63) + ' ');
      case 4:
        buffer[base + 3] = (byte) (((n >> 15) & 31) + ' ');
      case 3:
        buffer[base + 2] = (byte) (((n >> 10) & 95) + ' ');
      case 2:
        buffer[base + 1] = (byte) (((n >> 5) & 63) + ' ');
      case 1:
        buffer[base + 0] = (byte) (((n) & 31) + ' ');
      case 0:
        break;
    }
  }

}
