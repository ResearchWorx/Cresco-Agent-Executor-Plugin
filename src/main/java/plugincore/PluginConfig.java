package plugincore;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;

public class PluginConfig {

	private SubnodeConfiguration configObj; 
	  
	
	public PluginConfig(SubnodeConfiguration configObj) throws ConfigurationException
	{
	    this.configObj = configObj;
	}
	public Boolean webDb()
	{
		if(configObj.getString("webdb").equals("1"))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public Map<String,String> getPluginConfigMap()
	{
		final Map<String,String> result=new TreeMap<String,String>();
		  final Iterator it=configObj.getKeys();
		  while (it.hasNext()) {
		    final Object key=it.next();
		    final String value=configObj.getString(key.toString());
		    result.put(key.toString(),value);
		  }
		  return result;	
	}
	
	public String getPluginConfigString()
	{
		//final Map<String,String> result=new TreeMap<String,String>();
		  StringBuilder sb = new StringBuilder();
			final Iterator it=configObj.getKeys();
		  while (it.hasNext()) {
		    final Object key=it.next();
		    final String value=configObj.getString(key.toString());
		    //result.put(key.toString(),value);
		    sb.append(key.toString() + "=" + value + ",");
		  }
		  return sb.toString().substring(0, sb.length() -1);
		  //return result;	
	}
	
	public String getPluginName()
	{
		return configObj.getString("pluginname");
	}
	public String getRegion()
	{
		return configObj.getString("region");
	}
	public boolean getLogConsumerEnabled()
	{
		if(configObj.getString("enablelogconsumer").equals("1"))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	public int getWatchDogTimer()
	{
		return /*configObj.getInt("watchdogtimer")*/5000;
	}
	
	public String getPath(String path) {
		return configObj.getString(path);
	}
}