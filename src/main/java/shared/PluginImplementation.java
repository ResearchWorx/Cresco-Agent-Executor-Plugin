package shared;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.configuration.SubnodeConfiguration;

import plugincore.PluginEngine;


public class PluginImplementation implements PluginInterface {

    public PluginEngine pe;

    public PluginImplementation() {
        pe = new PluginEngine(); //actual plugin code
    }

    public PluginEngine getPE() { return pe; }

    public String getName() {
        return pe.getName();
    }

    public String getVersion() {
        return pe.getVersion();
    }

    public void msgIn(MsgEvent command) {
        pe.msgIn(command);
    }

    public boolean initialize(ConcurrentLinkedQueue<MsgEvent> msgOutQueue, ConcurrentLinkedQueue<MsgEvent> msgInQueue, SubnodeConfiguration configObj, String region, String agent, String plugin) {
        return pe.initialize(msgOutQueue, msgInQueue, configObj, region, agent, plugin);
    }

    public void shutdown() {
        pe.shutdown();
    }

}

