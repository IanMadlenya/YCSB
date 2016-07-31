package com.yahoo.ycsb;

import junit.framework.Assert;

import java.util.Arrays;

/**
 * Created by peter on 31/07/16.
 */
public class EncodeStringMain {
  public static void main(String[] args) {
    RandomByteIterator rbi = new RandomByteIterator(100);
    byte[] bytes = new byte[100];
    rbi.nextBuf(bytes, 0);
    System.out.println(new String(bytes));
    int[] ints = new int[(100 + 5) / 6];
    for (int i = 0; i < ints.length; i++) {
      ints[i] = getInt(bytes, i * 6);
    }
    System.out.println(Arrays.toString(ints));
    byte[] bytes2 = new byte[100];
    for (int i = 0; i < ints.length; i++)
      getBytes(bytes2, i * 6, ints[i]);
    System.out.println(new String(bytes2));
    Assert.assertEquals(new String(bytes), new String(bytes2));
  }

  static int getInt(byte[] bytes, int offset) {
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

  static void getBytes(byte[] buffer, int base, int bytes) {
    switch (buffer.length - base) {
      default:
        buffer[base + 5] = (byte) (((bytes >> 25) & 95) + ' ');
      case 5:
        buffer[base + 4] = (byte) (((bytes >> 20) & 63) + ' ');
      case 4:
        buffer[base + 3] = (byte) (((bytes >> 15) & 31) + ' ');
      case 3:
        buffer[base + 2] = (byte) (((bytes >> 10) & 95) + ' ');
      case 2:
        buffer[base + 1] = (byte) (((bytes >> 5) & 63) + ' ');
      case 1:
        buffer[base + 0] = (byte) (((bytes) & 31) + ' ');
      case 0:
        break;
    }
  }
}
