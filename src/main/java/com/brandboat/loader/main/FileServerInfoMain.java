/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.brandboat.loader.main;

import com.brandboat.loader.AbstractDB;
import com.brandboat.loader.lib.FileServerInfo;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author brandboat
 */
public class FileServerInfoMain {
  private static final int TOTAL_THREAD = 1;
  public static void main(String[] args)
    throws SQLException, InterruptedException {
    CountDownLatch latch = new CountDownLatch(TOTAL_THREAD);
    ExecutorService executor = Executors.newFixedThreadPool(TOTAL_THREAD);
    for (int i = 0; i < TOTAL_THREAD; i++) {
      AbstractDB adb1 = new FileServerInfo(latch, "jdbc:phoenix:192.168.0.103:2181");
      executor.execute(adb1);
    }
    latch.await();
    executor.shutdownNow();
  }
}
