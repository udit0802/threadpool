package com.prod.engg.threading;

import static java.lang.Math.acos;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.toRadians;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.json.JSONException;
import org.json.JSONObject;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value.GeoJSONValue;

public class WorkerThread implements Runnable {
	
	private String message;  
    public WorkerThread(String s){  
        this.message=s;  
    } 
    
    public static double distance(double lat1,double lon1,double lat2,double lon2) {
		return acos(sin(toRadians(lat1))*sin(toRadians(lat2)) + cos(toRadians(lat1))*cos(toRadians(lat2))*cos(toRadians(lon1-lon2)))*6371;
	}

	public void run() {  
        
		System.out.println(Thread.currentThread().getName());
        AerospikeClient client = new AerospikeClient("10.5.213.43", 3000);
		Key key = new Key("FIBERFORCE_TRACKING", "TrackingMap", message);
		Record record = client.get(null, key);
		if(record == null)
			return;
		TreeMap<Long, GeoJSONValue> map=(TreeMap<Long, GeoJSONValue>) record.bins.get("coordinates");
		Long start = System.currentTimeMillis()-1800000;
		Long end = System.currentTimeMillis();
		SortedMap<Long, GeoJSONValue> sortedMap = (SortedMap<Long, GeoJSONValue>) map.subMap(start, end);
		if(sortedMap.isEmpty())
			return;
		System.out.println("start = " + start + ", end = " + end);
		double dist = 0;
		double prevLat = 0;
		double prevLon = 0;
		double lat = 0;
		double lon = 0;
		int count = 1;

		for(Map.Entry<Long, GeoJSONValue> entry : sortedMap.entrySet()) {
			String routeInfo = entry.getValue().toString();
			JSONObject jsonObj = null;
			try {
				jsonObj = new JSONObject(routeInfo);
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			if(count == 1) {
				try {
					prevLat = (Double) jsonObj.getJSONArray("coordinates").get(0);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					prevLon = (Double) jsonObj.getJSONArray("coordinates").get(1);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				count++;
				continue;
			}
			try {
				lat = (Double) jsonObj.getJSONArray("coordinates").get(0);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				lon = (Double) jsonObj.getJSONArray("coordinates").get(1);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			dist += distance(prevLat, prevLon, lat, lon);
			prevLat = lat;
			prevLon = lon;
		}
        try {
            System.out.println("message = " + message + " dist = " + dist);
        } catch (Exception e) {
        }
    
	}


}
