/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
*/
package com.brandboat.loader;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 *
 * @author brandboat
 */
public abstract class AbstractDB implements Closeable, Runnable {

  private final CountDownLatch latch;

  public AbstractDB(CountDownLatch latch, String connURL) {
    this.latch = latch;
  }

  public abstract void createTableIfNotExist();

  protected abstract void insertOneRow();

  protected abstract void insertMultiRow(int n);

  protected abstract void innerRun();

  @Override
  public void run() {
    try {
      innerRun();
    } finally {
      latch.countDown();
      try {
        close();
      } catch (IOException ex) {
      }
    }
  }
}
