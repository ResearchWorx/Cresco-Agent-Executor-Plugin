package shared;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.configuration.SubnodeConfiguration;

interface PluginInterface {

    boolean initialize(ConcurrentLinkedQueue<MsgEvent> msgOutQueue, ConcurrentLinkedQueue<MsgEvent> msgInQueue, SubnodeConfiguration configObj, String region, String agent, String plugin);

    void msgIn(MsgEvent command);

    String getName();

    String getVersion();

    void shutdown();
}


