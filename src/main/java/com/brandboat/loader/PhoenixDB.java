/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.brandboat.loader;

import java.util.concurrent.CountDownLatch;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author brandboat
 */
public abstract class PhoenixDB extends AbstractDB {

  public PhoenixDB(CountDownLatch latch, String connURL) {
    super(latch, connURL);
  }

  public static String toTimeStamp(DateTime dt) {
    DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss");
    return dt.toString(dtf);
  }

}
