package com.brandboat.loader.main;

import com.brandboat.loader.AbstractDB;
import com.brandboat.loader.lib.FdcRunData;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
 */
/**
 *
 * @author brandboat
 */
public class FdcRunDataMain {

  private static final int TOTAL_THREAD = 20;

  public static void main(String[] args)
      throws SQLException, InterruptedException, IOException {
    CountDownLatch latch = new CountDownLatch(TOTAL_THREAD);
    ExecutorService executor = Executors.newFixedThreadPool(TOTAL_THREAD);
    for (int i = 0; i < TOTAL_THREAD; i++) {
      AbstractDB adb1 = new FdcRunData(latch, "jdbc:phoenix:192.168.0.103,192.168.0.104,192.168.0.105:2181",
              "UPSERT INTO FDC.FDC_RUN_DATA(endTmst,fab,module_Id,run_No,subRun_No,param_Ver,startTmst,lot,"
                      + "wafer_No,wafer_Id,cassette,portId,recipe,recipe2,product,process,route,step,layer,lottype,"
                      + "mes_Recipe1,mes_Recipe2,server,path,tool_Name,module_Name) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
      executor.execute(adb1);
    }
    latch.await();
    executor.shutdownNow();
  }
}
