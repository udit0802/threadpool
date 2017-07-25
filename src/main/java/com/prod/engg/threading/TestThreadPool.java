package com.prod.engg.threading;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestThreadPool {

	public static void main(String[] args) { 
		
		ArrayList<String> travelIds = new ArrayList<String>();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			String url = "jdbc:oracle:thin:@10.5.203.22:1528:test";
			String userName = "FF_USER";
			String password = "FF_USER";
			Connection con=DriverManager.getConnection(url,userName,password);
			
			String query = "select T.TRACKING_ID from TRIP_MASTER T where T.STATUS='START' AND T.START_DATE > SYSDATE-NUMTODSINTERVAL(24,'HOUR')";
			PreparedStatement stmt=con.prepareStatement(query);
			ResultSet rs=stmt.executeQuery();
			while(rs.next()){ 
				String travelId = rs.getString(1);
				System.out.println(travelId); 
				travelIds.add(travelId);
				}  
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		
        ExecutorService executor = Executors.newFixedThreadPool(travelIds.size());//creating a pool of 5 threads
//        ArrayList<String> travelIds = new ArrayList<String>();
//        travelIds.add("659T1500888976656");
//        travelIds.add("656T1500729918623");
//        travelIds.add("646T1500890359225");
//        travelIds.add("649T1500721101681");
//        travelIds.add("649T1500889686173");
        for (String travelId:travelIds) {  
            Runnable worker = new WorkerThread("" + travelId);  
            executor.execute(worker);//calling execute method of ExecutorService  
          }  
        executor.shutdown();  
        while (!executor.isTerminated()) {   }  
  
        System.out.println("Finished all threads");  
    }  
}
