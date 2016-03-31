package plugincore;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.MalformedInputException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

import org.apache.commons.configuration.SubnodeConfiguration;

import channels.RPCCall;
import shared.Clogger;
import shared.MsgEvent;
import shared.MsgEventType;
import shared.PluginImplementation;

public class PluginEngine {

    private static CommandExec commandExec;
    private static WatchDog wd;

    static Clogger clog;
    static ConcurrentLinkedQueue<MsgEvent> logOutQueue;
    static PluginConfig config;
    static String pluginName;
    static String pluginVersion;

    public static String plugin;
    public static String agent;
    public static String region;
    public static RPCCall rpcc;
    public static Map<String, MsgEvent> rpcMap;
    //public static ConcurrentLinkedQueue<MsgEvent> msgOutQueue;
    public static ConcurrentLinkedQueue<MsgEvent> msgInQueue;

    public PluginEngine() {
        pluginName = "cresco-agent-executor-plugin";
        clog = new Clogger(new ConcurrentLinkedQueue<MsgEvent>(), "init", "init", pluginName, Clogger.Level.Info);
    }

    public static void shutdown() {
        clog.info("Plugin Shutdown : Agent=" + agent + "pluginname=" + plugin);
        wd.timer.cancel(); //prevent rediscovery
        try {
            MsgEvent me = new MsgEvent(MsgEventType.CONFIG, region, null, null, "disabled");
            me.setParam("src_region", region);
            me.setParam("src_agent", agent);
            me.setParam("src_plugin", plugin);
            me.setParam("dst_region", region);

            //msgOutQueue.offer(me);
            msgInQueue.offer(me);
            //PluginEngine.rpcc.call(me);
            clog.debug("Sent disable message");
        } catch (Exception ex) {
            String msg2 = "Plugin Shutdown Failed: Agent=" + agent + "pluginname=" + plugin;
            clog.error(msg2);
        }
    }

    public static String getName() {
        return pluginName;
    }

    public static String getVersion() {
        String version;
        try {
            String jarFile = PluginImplementation.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            File file = new File(jarFile.substring(5, (jarFile.length() - 2)));
            FileInputStream fis = new FileInputStream(file);
            @SuppressWarnings("resource")
            JarInputStream jarStream = new JarInputStream(fis);
            Manifest mf = jarStream.getManifest();

            Attributes mainAttribs = mf.getMainAttributes();
            version = mainAttribs.getValue("Implementation-Version");
        } catch (Exception ex) {
            String msg = "Unable to determine Plugin Version " + ex.toString();
            clog.error(msg);
            version = "Unable to determine Version";
        }

        return pluginName + "." + version;

    }

    //steps to init the plugin
    public boolean initialize(ConcurrentLinkedQueue<MsgEvent> outQueue, ConcurrentLinkedQueue<MsgEvent> inQueue, SubnodeConfiguration configObj, String newRegion, String newAgent, String newPlugin) {
        //create logger
        clog = new Clogger(inQueue, newRegion, newAgent, newPlugin, Clogger.Level.Info);

        clog.trace("Call to initialize");
        clog.trace("Building rpcMap");
        rpcMap = new ConcurrentHashMap<>();
        clog.trace("Building rpcc");
        rpcc = new RPCCall();

        clog.trace("Building commandExec");
        commandExec = new CommandExec();


        //clog.trace("Building msgOutQueue");
        //ConcurrentLinkedQueue<MsgEvent> msgOutQueue = outQueue;
        clog.trace("Setting msgInQueue");
        msgInQueue = inQueue; //messages to agent should go here

        clog.trace("Setting Region");
        region = newRegion;
        clog.trace("Setting Agent");
        agent = newAgent;
        clog.trace("Setting Plugin");
        plugin = newPlugin;

        try {
            clog.trace("Building logOutQueue");
            logOutQueue = new ConcurrentLinkedQueue<>(); //create our own queue

            clog.trace("Checking msgInQueue");
            if (msgInQueue == null) {
                System.out.println("MsgInQueue==null");
                return false;
            }

            clog.trace("Building new PluginConfig");
            config = new PluginConfig(configObj);

            clog.info("Starting Executor Plugin");
            try {
                boolean folderReady = false;
                URL jarURL = PluginEngine.class.getProtectionDomain().getCodeSource().getLocation();
                String jarPath = URLDecoder.decode(jarURL.getFile(), "UTF-8");
                String parentPath = new File(jarPath).getParentFile().getPath();
                parentPath = parentPath.replace("file:","") + File.separator;
                String folder = parentPath + pluginName;
                File file = new File(folder);
                if (!file.exists()) {
                    if (file.mkdir()) {
                        folderReady = true;
                    } else {
                        clog.error("Failed to create: {}", folder);
                    }
                } else {
                    clog.info("Already exists: {}", folder);
                    folderReady = true;
                }

                if (folderReady) {
                    folder += File.separator;
                    boolean commandReady;
                    String url = config.getPath("url");
                    if (url != null) {
                        String downloadFile = folder + fileName(url);
                        commandReady = downloadFile(url, downloadFile);
                        if (url.endsWith(".tar")) {
                            executeCommand("tar xf " + downloadFile + " -C " + folder);
                        } else if (url.endsWith(".tar.gz")) {
                            executeCommand("tar xzf " + downloadFile + " -C " + folder);
                        } else if (url.endsWith(".zip")) {
                            executeCommand("unzip -uq " + downloadFile + " -d " + folder);
                        }
                        String executable = config.getPath("executable");
                        if (executable != null) {
                            if ((new File(folder + executable)).exists()) {
                                executeCommand("chmod a+x " + folder + executable);
                            } else {
                                commandReady = false;
                                clog.error("Executable file does not exist");
                            }
                        } else {
                            commandReady = false;
                            clog.error("No [executable] entry found");
                        }
                        if (commandReady) {
                            String args = config.getPath("args");
                            if (args != null) {
                                clog.info("Executing: {}", folder + executable + " " + args);
                                executeCommand(folder + executable + " " + args);
                            } else {
                                clog.info("Executing: {}", folder + executable);
                                executeCommand(folder + executable);
                            }
                        } else {
                            clog.error("Errors encountered downloading and processing file");
                        }
                    }
                } else {
                    clog.error("Errors encountered generating plugin downloads folder");
                }

                clog.info("Completed Plugin Routine");


            } catch (Exception ex) {
                clog.error("Failed to load Plugin Execution Segment {}", ex.toString());
                return false;
            }

            clog.trace("Starting WatchDog");
            wd = new WatchDog();
            clog.trace("Successfully started plugin");
            return true;
        } catch (Exception ex) {
            String msg = "ERROR IN PLUGIN: : Region=" + region + " Agent=" + agent + " plugin=" + plugin + " " + ex.getMessage();
            ex.printStackTrace();
            clog.error("initialize {}", msg);
            //clog.error(msg);
            return false;
        }
    }

    private boolean downloadFile(String url, String to) {
        try {
            ReadableByteChannel in = Channels.newChannel(new URL(url).openStream());
            FileChannel out = new FileOutputStream(to).getChannel();
            out.transferFrom(in, 0, Long.MAX_VALUE);
            return true;
        } catch (MalformedURLException mue) {
            // WHAT!?! DO SOMETHIN'!
            return false;
        } catch (IOException ioe) {
            // WHAT!?! DO SOMETHIN'!
            return false;
        } catch (Exception e) {
            // WHAT!?! DO SOMETHIN'!
            return false;
        }
    }

    private String fileName(String url) {
        return url.substring(url.lastIndexOf("/") + 1);
    }

    private void executeCommand(String command) {
        StringBuffer output = new StringBuffer();
        StringBuffer error = new StringBuffer();
        Process p;
        try {
            p = Runtime.getRuntime().exec(command);
            p.waitFor();
            BufferedReader outputFeed = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String outputLine;
            while ((outputLine = outputFeed.readLine()) != null) {
                output.append(outputLine);
            }
            if (!output.toString().equals(""))
                clog.info(output.toString());
            BufferedReader errorFeed = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String errorLine;
            while ((errorLine = errorFeed.readLine()) != null) {
                error.append(errorLine);
            }
            if (!error.toString().equals(""))
                clog.error(error.toString());
        } catch (IOException ioe) {
            // WHAT!?! DO SOMETHIN'!
        } catch (InterruptedException ie) {
            // WHAT!?! DO SOMETHIN'!
        } catch (Exception e) {
            // WHAT!?! DO SOMETHIN'!
        }
    }

    public static void msgIn(MsgEvent me) {
        final MsgEvent ce = me;
        try {
            Thread thread = new Thread() {
                public void run() {
                    try {
                        MsgEvent re = commandExec.cmdExec(ce);
                        if (re != null) {
                            re.setReturn(); //reverse to-from for return
                            msgInQueue.offer(re); //send message back to queue
                        }

                    } catch (Exception ex) {
                        clog.error("Controller : PluginEngine : msgIn Thread: {}", ex.toString());
                    }
                }
            };
            thread.start();
        } catch (Exception ex) {
            clog.error("Controller : PluginEngine : msgIn Thread: {}", ex.toString());
        }
    }
}
