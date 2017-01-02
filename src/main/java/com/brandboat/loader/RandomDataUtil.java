/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.brandboat.loader;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.joda.time.DateTime;

/**
 *
 * @author brandboat
 */
public final class RandomDataUtil {
  
  private RandomDataUtil() {}
  
  private static Random r = new Random();
  
  public static int randomInt() {
    return r.nextInt();
  }
  
  public static long randomLong() {
    return r.nextLong();
  }
  
  public static String randomString(int n) {
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < n; i++) {
      char c = (char) ('a' + r.nextInt(26));
      sb.append(c);
    }
    return sb.toString();
  }
  
  public static DateTime randomDateTime(long min, long max) {
    long l = ThreadLocalRandom.current().nextLong(min, max);
    DateTime dt = new DateTime(l);
    return dt;
  }
  
  public static DateTime randomDateTime(DateTime minDt, DateTime maxDt) {
    long min = minDt.getMillis();
    long max = maxDt.getMillis();
    return randomDateTime(min, max);
  }
}
