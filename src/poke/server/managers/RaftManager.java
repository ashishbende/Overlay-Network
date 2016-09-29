package poke.server.managers;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.beans.Beans;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.comm.App.ClientMessage;
import poke.comm.App.Details;
import poke.comm.App.Header;
import poke.comm.App.JoinMessage;
import poke.comm.App.Payload;
import poke.comm.App.Request;
import poke.comm.App.ClientMessage.Functionalities;
import poke.comm.App.ClientMessage.MessageType;
import poke.comm.App.ClientMessage.Requester;
import poke.comm.App.ClusterMessage;
import poke.comm.App.Header.Routing;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftMessage;
import poke.core.Mgmt.RaftMessage.RaftAction;
import poke.server.ServerInitializer;
import poke.server.conf.ClusterConfList;
import poke.server.conf.JsonUtil;
import poke.server.conf.ClusterConfList.ClusterConf;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.election.Election;
import poke.server.election.ElectionListener;
import poke.server.election.LogMessage;
import poke.server.election.RaftElection;
import poke.server.managers.ConnectionManager.connectionState;
import poke.server.resources.ResourceFactory;

public class RaftManager implements ElectionListener {

	protected static Logger logger = LoggerFactory.getLogger("election");
	protected static AtomicReference<RaftManager> instance = new AtomicReference<RaftManager>();

	private static ServerConf conf;
	private static ClusterConfList clusterConf;
	private static long lastKnownBeat = System.currentTimeMillis();
	// number of times we try to get the leader when a node starts up
	private int firstTime = 2;
	/** The election that is in progress - only ONE! */
	private Election election;
	private int electionCycle = -1;
	private Integer syncPt = 1;
	/** The leader */
	Integer leaderNode;
	public static Integer self;
	public static final Integer selfClusterId = 1;

	public static RaftManager initManager(ServerConf conf, ClusterConfList clusterConfList) {
		RaftManager.conf = conf;
		RaftManager.clusterConf = clusterConfList;
		instance.compareAndSet(null, new RaftManager());
		self = conf.getNodeId();

		return instance.get();
	}

	// process request from other clusters
	public void processRequest(Management mgmt) {
		if (!mgmt.hasRaftmessage())
			return;
		RaftMessage rm = mgmt.getRaftmessage();
		// when a new node joins the network it will want to know who the leader
		// is - we kind of ram this request-response in the process request
		// though there has to be a better place for it
		if (rm.getRaftAction().getNumber() == RaftAction.WHOISTHELEADER_VALUE) {
			respondToWhoIsTheLeader(mgmt);
			return;
		}
		// else respond to the message using process Function in RaftElection

		Management rtn = electionInstance().process(mgmt);
		if (rtn != null)
			ConnectionManager.broadcastAndFlush(rtn);
	}

	/**
	 * check the health of the leader (usually called after a HB update)
	 * 
	 * @param mgmt
	 */
	public boolean assessCurrentState() {
		// give it two tries to get the leader else return true to start
		// election
		if (firstTime > 0) {
			this.firstTime--;
			logger.info("In assessCurrentState() if condition");
			askWhoIsTheLeader();
			return false;
		} else
			return true;// starts Elections
	}

	// tell the requester who is the leader
	private void askWhoIsTheLeader() {
		logger.info("Node " + conf.getNodeId() + " is searching for the leader");
		if (whoIsTheLeader() == null) {
			logger.info("----> I cannot find the leader is! I don't know!");
			return;
		} else {
			logger.info("The Leader is " + this.leaderNode);
		}

	}

	// if leader is not present return
	// else send a message to get the leader node.
	private void respondToWhoIsTheLeader(Management mgmt) {
		if (this.leaderNode == null) {
			logger.info("----> I cannot respond to who the leader is! I don't know!");
			return;
		}
		logger.info("Node " + conf.getNodeId() + " is replying to " + mgmt.getHeader().getOriginator()
				+ "'s request who the leader is. Its Node " + this.leaderNode);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999);

		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setLeader(this.leaderNode);
		rmb.setRaftAction(RaftAction.THELEADERIS);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftmessage(rmb);
		try {

			Channel ch = ConnectionManager.getConnection(mgmt.getHeader().getOriginator(), connectionState.SERVERMGMT);
			if (ch != null)
				ch.writeAndFlush(mb.build());

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	// making an instance of raft election
	private Election electionInstance() {
		if (election == null) {
			synchronized (syncPt) {
				if (election != null)
					return election;

				// new election
				String clazz = RaftManager.conf.getElectionImplementation();

				// if an election instance already existed, this would
				// override the current election
				try {
					election = (Election) Beans.instantiate(this.getClass().getClassLoader(), clazz);
					election.setNodeId(conf.getNodeId());
					election.setListener(this);
				} catch (Exception e) {
					logger.error("Failed to create " + clazz, e);
				}
			}
		}

		return election;

	}

	/*
	 * This function will call the run method of Raft Monitor class in the
	 * election instance. In our case RaftElection --> Raft Monitor
	 */
	public void startMonitor() {
		logger.info("Raft Monitor Started ");
		if (election == null)
			((RaftElection) electionInstance()).getMonitor().start();

		new Thread(new ClusterConnectionManager()).start();
	}

	public Integer whoIsTheLeader() {
		return this.leaderNode;
	}

	/** election listener implementation */
	public void concludeWith(boolean success, Integer leaderID) {
		if (success) {
			logger.info("----> the leader is " + leaderID);
			this.leaderNode = leaderID;
		}

		election.clear();
	}

	// updating cluster configuration file
	public void updateconfigfile() {
		//ConnectionManager.removeAllSeverAppConnection();
		generateNewClusterConfigList();
	}
	
	// method for generating new cluster config list
	@SuppressWarnings("resource")
	public void generateNewClusterConfigList(){
		File clusterCfg = new File("~/runtime/cluster.conf");
		BufferedInputStream br = null;
		try {
			byte[] raw = new byte[(int) clusterCfg.length()];
			new BufferedInputStream(new FileInputStream(clusterCfg)).read(raw);
			clusterConf = JsonUtil.decode(new String(raw),
					ClusterConfList.class);

			ResourceFactory.initializeCluster(clusterConf);
			// updating cluster details to connect to the new value
			ClusterConnectionManager c = new ClusterConnectionManager();
			c.UpdateClusterDetails(clusterConf);

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	// send update list message to all local nodes in the cluster.
	public void sendUpdateListMessage() {
		String ip;
		try {
			ip = InetAddress.getLocalHost().toString();
			UpdateManager.initializeServerConfig(ip, conf.getPort(), conf.getNodeId(), selfClusterId.toString());
			for (NodeDesc nn : conf.getAdjacent().getAdjacentNodes().values()) {
				ConnectionManager.sendToLocalNode(
						sendToAllLocalNodes(ip, conf.getPort(), conf.getNodeId(), selfClusterId.toString()),
						nn.getNodeId());
			}

			updateconfigfile();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// building of message which will be send to update cluster conf file
	public Request sendToAllLocalNodes(String host, int port, int nodeId, String clusterId) {

		// Build Header
		Header.Builder h = Header.newBuilder();
		h.setRoutingId(Routing.JOBS);
		h.setOriginator(self);
		h.setTime(System.currentTimeMillis());
		// NOt setting Poke Status, Reply Message, ROuting Path, Name
		// Value set
		// TO Node is the node to which this client will get connected
		h.setToNode(self);
		// Now send back the reply
		Request.Builder r = Request.newBuilder();
		ClientMessage.Builder clBuilder = ClientMessage.newBuilder();
		clBuilder.setMessageType(MessageType.REPLICATIONLOCAL);
		clBuilder.setHost(host);
		clBuilder.setPort(port);
		clBuilder.setNodeid(nodeId);
		clBuilder.setClusterid(clusterId);

		// Build Payload
		Payload.Builder p = Payload.newBuilder();
		// Not adding anything as it is just a register message
		p.setClientMessage(clBuilder);

		r.setBody(p);
		r.setHeader(h);
		Request req = r.build();
		return req;
	}

	// creating logs for the leader
	public void createLogs(String name) {
		LogMessage lm = ((RaftElection) electionInstance()).getLm();
		Integer logIndex = lm.getLogIndex();
		LinkedHashMap<Integer, String> entries = lm.getEntries();
		lm.setPrevLogIndex(logIndex);
		lm.setLogIndex(++logIndex);
		entries.put(logIndex, name);
		lm.setEntries(entries);
		((RaftElection) electionInstance()).setAppendLogs(true);

	}

	/*
	 * // start replication process // build message for all adjacent nodes
	 * 
	 * public boolean startReplication(poke.comm.App.Request request){ try{
	 * for(NodeDesc nn : conf.getAdjacent().getAdjacentNodes().values()){
	 * BuildReplicationMessage(request, nn.getNodeId()); }
	 * 
	 * System.out.println("Replication message sent to all nodes."); return
	 * true; } catch(Exception ex){ System.out.println(ex.getMessage()); return
	 * false; } }
	 * 
	 * // build message for replication public void
	 * BuildReplicationMessage(Request request, int toNode){ Header reqHeader =
	 * request.getHeader(); ClientMessage reqClientMsg =
	 * request.getBody().getClientMessage();
	 * 
	 * ClientMessage.Builder cBuilder = ClientMessage.newBuilder();
	 * cBuilder.setMsgId(reqClientMsg.getMsgId());
	 * cBuilder.setSenderId(RaftManager.getInstance().self);
	 * cBuilder.setReceiverId(RaftManager.getInstance().whoIsTheLeader());
	 * 
	 * if (reqClientMsg.hasDetails()) { Details.Builder dBuilder =
	 * Details.newBuilder();
	 * dBuilder.setCourseId(reqClientMsg.getDetails().getCourseId());
	 * dBuilder.setUserId(reqClientMsg.getDetails().getUserId());
	 * dBuilder.setUsername(reqClientMsg.getDetails().getUsername());
	 * dBuilder.setCourseName(reqClientMsg.getDetails().getCourseName());
	 * cBuilder.setDetails(dBuilder);
	 * 
	 * cBuilder.setFunctionality(reqClientMsg.getFunctionality());
	 * cBuilder.setMessageType(MessageType.REPLICATION);
	 * cBuilder.setRequestType(reqClientMsg.getRequestType());
	 * cBuilder.setBroadcastInternal(true);
	 * cBuilder.setCameFrom(Requester.FOLLOWER); }
	 * 
	 * Header.Builder h = Header.newBuilder();
	 * 
	 * h.setRoutingId(Routing.JOBS); h.setOriginator(reqHeader.getOriginator());
	 * h.setTag(reqHeader.getTag()); h.setTime(System.currentTimeMillis());
	 * h.setToNode(reqHeader.getToNode());
	 * 
	 * // add client msg to body Payload.Builder payload = Payload.newBuilder();
	 * payload.setClientMessage(cBuilder);
	 * 
	 * // add body to request Request.Builder req = Request.newBuilder();
	 * req.setBody(payload); req.setHeader(h); Request r = req.build();
	 * ConnectionManager.sendToNode(r, toNode); }
	 */

	/*
	 * public void sendResult(Request request){ ClusterConnectionManager mg =
	 * new ClusterConnectionManager(); List<NodeDesc> nodes =
	 * mg.clusterMap.get(1).getClusterNodes();
	 * 
	 * for (NodeDesc n : nodes) { ConnectionManager.sendToNode(request,
	 * n.getNodeId()); } }
	 */

	// it broadcasts the message to all the leaders of the clusters connected to
	// the respective cluster.
	public void sendMessageToOtherCluster(Request request, int originator) {
		System.out.println("broadcasting values to all the clusters");

		ClientMessage msg = request.getBody().getClientMessage();

		ClientMessage.Builder bld = ClientMessage.newBuilder();

		bld.setSenderId(RaftManager.self);
		bld.setReceiverId(msg.getReceiverId());
		if (msg.hasDetails()) {
			Details.Builder dBuilder = Details.newBuilder();
			dBuilder.setCourseId(msg.getDetails().getCourseId());
			dBuilder.setUserId(msg.getDetails().getUserId());
			dBuilder.setUsername(msg.getDetails().getUsername());
			dBuilder.setCourseName(msg.getDetails().getCourseName());
			bld.setDetails(dBuilder);

			bld.setFunctionality(msg.getFunctionality());
			bld.setRequestType(msg.getRequestType());
			bld.setClientId(msg.getClientId());
		}

		ClusterMessage.Builder clMsg = ClusterMessage.newBuilder();
		clMsg.setClientMessage(bld);
		clMsg.setClusterId(self);

		Header head = request.getHeader();
		// Now send back the reply
		Request.Builder r = Request.newBuilder();

		// Build Header
		Header.Builder h = Header.newBuilder();
		h.setRoutingId(Routing.JOBS);
		h.setOriginator(originator);
		System.out.println("ORIGINATOR IS " + head.getOriginator());
		h.setTime(System.currentTimeMillis());
		// NOt setting Poke Status, Reply Message, ROuting Path, Name Value set
		// TO Node is the node to which this client will get connected
		h.setToNode(head.getOriginator());

		// Build Payload
		Payload.Builder p = Payload.newBuilder();
		// Not adding anything as it is just a register message
		p.setClusterMessage(clMsg);

		r.setBody(p);
		r.setHeader(h);
		Request req = r.build();
		ConnectionManager.broadcastServers(req);
	}

	// this function adds a channel between all the local nodes of the cluster
	public void addChannelsforLocalNodes() {
		System.out.println("adding local nodes to cluster nodes after election");
		for (NodeDesc nn : conf.getAdjacent().getAdjacentNodes().values()) {
			if (RaftManager.getInstance().whoIsTheLeader() == nn.getNodeId()) {
				AddLocalNodeConnection(nn.getHost(), nn.getPort(), nn.getNodeId());
			}
		}
	}

	// Ashok

	// added channels for local connection.
	public void AddLocalNodeConnection(String host_host, int host_port, int nodeID) {
		String host = host_host;
		int port = host_port;

		ChannelFuture channel = connectNodes(host, port);
		if (channel != null) {
			if (channel.channel().isWritable()) {
				System.out.println("CHANNEL WRITABLE.");
				ConnectionManager.addConnection(nodeID, channel.channel(), connectionState.LOCALSERVERAPP);
				logger.info("Local Nodes added in local configuretion " + nodeID + " added");
			}
		} else
			System.out.println("CHANNEL IS NULL for NODE " + nodeID);
	}

	// This method builds a channel for host and port.
	public ChannelFuture connectNodes(String host, int port) {

		ChannelFuture channel = null;
		EventLoopGroup workerGroup = new NioEventLoopGroup();

		try {
			// logger.info("Attempting to connect to : "+host+" : "+port);
			Bootstrap b = new Bootstrap();
			b.group(workerGroup).channel(NioSocketChannel.class).handler(new ServerInitializer(false));

			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			channel = b.connect(host, port).syncUninterruptibly();
		} catch (Exception e) {
			// e.printStackTrace();
			return null;
		}

		return channel;
	}

	// get set method for the class
	public static Logger getLogger() {
		return logger;
	}

	public static void setLogger(Logger logger) {
		RaftManager.logger = logger;
	}

	public static RaftManager getInstance() {
		return instance.get();
	}

	public static void setInstance(AtomicReference<RaftManager> instance) {
		RaftManager.instance = instance;
	}

	public static ServerConf getConf() {
		return conf;
	}

	public static void setConf(ServerConf conf) {
		RaftManager.conf = conf;
	}

	public static long getLastKnownBeat() {
		return lastKnownBeat;
	}

	public static void setLastKnownBeat(long lastKnownBeat) {
		RaftManager.lastKnownBeat = lastKnownBeat;
	}

	public int getFirstTime() {
		return firstTime;
	}

	public void setFirstTime(int firstTime) {
		this.firstTime = firstTime;
	}

	public Election getElection() {
		return election;
	}

	public void setElection(Election election) {
		this.election = election;
	}

	public int getElectionCycle() {
		return electionCycle;
	}

	public void setElectionCycle(int electionCycle) {
		this.electionCycle = electionCycle;
	}

	public Integer getSyncPt() {
		return syncPt;
	}

	public void setSyncPt(Integer syncPt) {
		this.syncPt = syncPt;
	}

	public Integer getLeaderNode() {
		return leaderNode;
	}

	public void setLeaderNode(Integer leaderNode) {
		this.leaderNode = leaderNode;
	}

	/*
	 * Once the leader is elected This thread will continuosly send join message
	 * to all the nodes in the Cluster conf file.
	 ***/
	public class ClusterConnectionManager extends Thread {
		private Map<Integer, Channel> connMap = new HashMap<Integer, Channel>();
		private Map<Integer, ClusterConf> clusterMap;

		// initialize cluster map for getting all clusters
		public ClusterConnectionManager() {
			clusterMap = clusterConf.getClusters();
		}

		// updating cluster map for gettting new values
		public void UpdateClusterDetails(ClusterConfList clusterConfigValue) {
			clusterMap = clusterConfigValue.getClusters();
		}

		// registering connection for cluster nodes
		public void registerConnection(int nodeId, Channel channel) {
			connMap.put(nodeId, channel);
		}

		// making a channel between the cluster nodes.
		public ChannelFuture connect(String host, int port) {

			ChannelFuture channel = null;
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			try {
				// logger.info("Attempting to connect to : "+host+" : "+port);
				Bootstrap b = new Bootstrap();
				b.group(workerGroup).channel(NioSocketChannel.class).handler(new ServerInitializer(false));

				b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
				b.option(ChannelOption.TCP_NODELAY, true);
				b.option(ChannelOption.SO_KEEPALIVE, true);

				channel = b.connect(host, port).syncUninterruptibly();
			} catch (Exception e) {
				// e.printStackTrace();
				return null;
			}

			return channel;
		}

		// making a cluster node join message
		public Request createClusterJoinMessage(int fromCluster, int fromNode, int toCluster, int toNode) {
			// logger.info("Creating join message");
			Request.Builder req = Request.newBuilder();

			JoinMessage.Builder jm = JoinMessage.newBuilder();
			jm.setFromClusterId(fromCluster);
			jm.setFromNodeId(fromNode);
			jm.setToClusterId(toCluster);
			jm.setToNodeId(toNode);

			req.setJoinMessage(jm.build());
			return req.build();

		}

		@Override
		public void run() {
			Iterator<Integer> it = clusterMap.keySet().iterator();
			while (true) {
				if (whoIsTheLeader() != null) {
					try {
						int key = it.next();
						// System.out.println("KEY IS - " + key);
						if (!connMap.containsKey(key)) {
							// System.out.println("CLUSTER IS GOING TO BE
							// ADDED");
							ClusterConf cc = clusterMap.get(key);
							List<NodeDesc> nodes = cc.getClusterNodes();

							// logger.info("For cluster "+ key +" nodes "+
							// nodes.size());

							for (NodeDesc n : nodes) {
								addClusterConfig(n.getPort(), n.getHost(), key, n.getNodeId());
							}
						}
					} catch (NoSuchElementException e) {
						// logger.info("Restarting iterations");
						it = clusterMap.keySet().iterator();
						try {
							Thread.sleep(3000);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
					}
				} else {
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}

		// method for adding all cluster nodes in cluster config file.
		public void addClusterConfig(int port_new, String host_new, int key, int nodeid) {
			String host = host_new;
			int port = port_new;

			ChannelFuture channel = connect(host, port);
			Request req = createClusterJoinMessage(RaftManager.selfClusterId, conf.getNodeId(), key,
					RaftManager.getInstance().whoIsTheLeader());
			if (channel != null) {
				 ConnectionManager.addConnection(nodeid, channel.channel(),
				 connectionState.SERVERAPP);
				channel = channel.channel().writeAndFlush(req);
				if (channel.channel().isWritable()) {
					registerConnection(key, channel.channel());
					logger.info("Connection to cluster " + key + " added");
				}
			}
		}

		// listener method
		public class ClusterLostListener implements ChannelFutureListener {
			ClusterConnectionManager ccm;

			public ClusterLostListener(ClusterConnectionManager ccm) {
				this.ccm = ccm;
			}

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				logger.info("Cluster " + future.channel() + " closed. Removing connection");
			}
		}
	}

}
