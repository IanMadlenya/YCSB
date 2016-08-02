package com.yahoo.ycsb.db;

import com.yahoo.ycsb.RandomByteIterator;
import org.junit.Assert;

import java.util.Arrays;

import static com.yahoo.ycsb.db.EncodeStringUtils.getInt;

/**
 * Created by peter on 31/07/16.
 */
public class EncodeStringUtilsTest {
  @org.junit.Test
  public void getBytes() throws Exception {
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
      EncodeStringUtils.getBytes(bytes2, i * 6, ints[i]);
    System.out.println(new String(bytes2));
    Assert.assertEquals(new String(bytes), new String(bytes2));
  }

}