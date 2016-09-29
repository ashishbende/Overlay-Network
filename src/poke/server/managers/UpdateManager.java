package poke.server.managers;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.Writer;
import org.json.JSONObject;

public class UpdateManager {
	static final String filepath = "/home/ashok/EclipseWorkspace/core-netty-4.4/runtime/cluster.conf";
	static JSONObject jsonObject = new JSONObject();
	
	@SuppressWarnings("deprecation")
	// method to change the cluster config file.
	public static void initializeServerConfig(String host, int port, int nodeid, String clusterid){
	    try {
	        File f = new File(filepath);
	        FileInputStream fis = null;
	        BufferedInputStream bis = null;
	        DataInputStream dis = null;
	        StringBuffer sb = new StringBuffer();
	        fis = new FileInputStream(f);
        	bis = new BufferedInputStream(fis);
        	dis = new DataInputStream(bis);
        	while(dis.available() != 0){
        		sb.append(dis.readLine());
        	}
	        
	        jsonObject = new JSONObject(sb.toString());
	        jsonObject.getJSONObject("clusters").getJSONObject(clusterid).getJSONArray("clusterNodes").getJSONObject(0).put("nodeId", nodeid);
	        jsonObject.getJSONObject("clusters").getJSONObject(clusterid).getJSONArray("clusterNodes").getJSONObject(0).put("port", port);
	        jsonObject.getJSONObject("clusters").getJSONObject(clusterid).getJSONArray("clusterNodes").getJSONObject(0).put("host", host);
	        jsonObject.getJSONObject("clusters").getJSONObject(clusterid).getJSONArray("clusterNodes").getJSONObject(0).put("nodeName", "node " + nodeid);
	        fis.close();
	        dis.close();
	        bis.close();
	        
	        Writer file = new FileWriter(filepath, false);
	        jsonObject.write(file);	
	        file.close();
	        RaftManager.getInstance().updateconfigfile();
	       
	    } catch(Exception e) {
	        e.printStackTrace();
	    }
	}
	
	/*
	// updates the cluster configuration file
	public void updateFile(String value){
		try{
			FileWriter file = new FileWriter(filepath, false);
			file.write(value);
			file.flush();
			file.close();
			RaftManager.getInstance().updateconfigfile();
		}
		catch(Exception ex){
			
		}
	}*/
}
