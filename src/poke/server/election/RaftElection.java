package poke.server.election;

/******
 * This file is used for Raft Consensus algorithm
 * @author ashok
 */

import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poke.core.Mgmt.LeaderElection;
import poke.core.Mgmt.LeaderElection.ElectAction;
import poke.core.Mgmt.LogEntries;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftMessage;
import poke.core.Mgmt.RaftMessage.RaftAction;
import poke.core.Mgmt.RaftMessage.RaftAppendAction;
import poke.server.managers.ConnectionManager;
import poke.server.managers.HeartbeatManager;
import poke.server.managers.RaftManager;
import poke.server.managers.UpdateManager;

public class RaftElection implements Election {

	protected static Logger logger = LoggerFactory.getLogger("Raft");
	private Integer nodeId;
	private ElectionListener listener;
	private ElectionState current;
	private int count = 0;
	private int term;
	public enum RState {
		Follower, Candidate, Leader
	}
	private RState currentState;
	LogMessage lm = new LogMessage();
	boolean appendLogs = false;

	private int leaderId = -1;
	private long lastKnownBeat = System.currentTimeMillis();
	private int timeElection;
	private RaftMonitor monitor = new RaftMonitor();
	private RaftMessage votedFor;
	
	//Default Constructor
	public RaftElection() {
		this.timeElection = new Random().nextInt(20000);
		if (this.timeElection < 16000)
			this.timeElection += 7000;
		logger.info("Election Timeout  " + timeElection);
		currentState = RState.Follower;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.election.Election#process(eye.Comm.LeaderElection)
	 * 
	 * @return The Management instance returned represents the message to send
	 * on. If null, no action is required. Not a great choice to convey to the
	 * caller; can be improved upon easily.
	 */
	public Management process(Management mgmt) {
		if (!mgmt.hasRaftmessage())
			return null;
		RaftMessage rm = mgmt.getRaftmessage();
		
		Management rtn = null;

		if (rm.getRaftAction().getNumber() == RaftAction.REQUESTVOTE_VALUE) {
			if (currentState == RState.Follower || currentState == RState.Candidate) {
				this.lastKnownBeat = System.currentTimeMillis();
				/**
				 * check if already voted for this term and candidate's log is atleast as upto
				 * date as receiver's log.
				 **/
				if ((this.votedFor == null
						|| rm.getTerm() > this.votedFor.getTerm()) && 
						(rm.getLogIndex() >= this.getLm().getLogIndex())) {
					if (this.votedFor != null) {
						System.out.println("Voting for "
								+ mgmt.getHeader().getOriginator()
								+ " for term" + rm.getTerm() + " from node "
								+ nodeId + " voted term "
								+ this.votedFor.getTerm());
					} 
					this.votedFor = rm;
					rtn = castVote();
				}
			} 
		} 
		else if (rm.getRaftAction().getNumber() == RaftAction.VOTE_VALUE) {
			if (currentState == RState.Candidate) {
				System.out.println("Node " + getNodeId()
						+ " received vote from Node "
						+ mgmt.getHeader().getOriginator() + ". Votecount "
						+ count);
				receiveVote(rm);
			} 
		} 
		else if (rm.getRaftAction().getNumber() == RaftAction.LEADER_VALUE) {
			if (rm.getTerm() >= this.term) {
				this.leaderId = mgmt.getHeader().getOriginator();
				this.term = rm.getTerm();
				this.lastKnownBeat = System.currentTimeMillis();
				notify1(true, mgmt.getHeader().getOriginator());
				logger.info("Node " + mgmt.getHeader().getOriginator()
						+ " is the leader ");
				RaftManager.getInstance().addChannelsforLocalNodes();
			}
			else{
				startElection();
			}
		} 
		else if (rm.getRaftAction().getNumber() == RaftAction.APPEND_VALUE) {
			if (currentState == RState.Candidate) {
				if (rm.getTerm() >= term) {
					this.lastKnownBeat = System.currentTimeMillis();
					this.term = rm.getTerm();
					this.leaderId = mgmt.getHeader().getOriginator();
					this.currentState = RState.Follower;
					logger.info("Received Append RPC from leader "
							+ mgmt.getHeader().getOriginator());
				}
			} 
			else if (currentState == RState.Follower) {
				this.term = rm.getTerm();
				this.lastKnownBeat = System.currentTimeMillis();
				logger.info("---Test--- " + mgmt.getHeader().getOriginator()
						+ "\n RaftAction=" + rm.getRaftAction().getNumber()
						+ " RaftAppendAction="
						+ rm.getRaftAppendAction().getNumber());
				RaftManager.getInstance().setLeaderNode(mgmt.getHeader().getOriginator());
				if (rm.getRaftAppendAction().getNumber() == RaftAppendAction.APPENDHEARTBEAT_VALUE) {
				
					logger.info("*Follower stateReceived AppendAction HB RPC from leader "
							+ mgmt.getHeader().getOriginator()
							+ "\n RaftAction="
							+ rm.getRaftAction().getNumber()
							+ " RaftAppendAction="
							+ rm.getRaftAppendAction().getNumber());
				}
				else if(rm.getRaftAppendAction().getNumber() == RaftAppendAction.APPENDLOG_VALUE){
					List<LogEntries> list = rm.getEntriesList();
					//Append logs from leader to follower hashmap 
					//from follower's prev
					for(int i = this.getLm().prevLogIndex+1 ;i < list.size();i++){
						LogEntries li = list.get(i);
						int tempindex = li.getLogIndex();
						String value = li.getLogData();
						this.getLm().getEntries().put(tempindex, value);
					}
					
				}
			} 
		} 
		return rtn;
	}
	
	// method for receiving vote from the other nodes
	// broadcasting leader message if got majority of votes
	private void receiveVote(RaftMessage rm) {
		logger.info("Size " + HeartbeatManager.getInstance().outgoingHB.size());
		if (++count > (HeartbeatManager.getInstance().outgoingHB.size() + 1) / 2) {
			logger.info("Final Count Received " + count);
			count = 0;
			currentState = RState.Leader;
			leaderId = this.nodeId;
			System.out.println(" Leader elected " + this.nodeId);
			notify1(true, this.nodeId);
			ConnectionManager.broadcastAndFlush(sendLeaderMesssage());
		}
	}

	// this method builds the leader message
	private Management sendLeaderMesssage() {
		RaftMessage.Builder rm = RaftMessage.newBuilder();
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security
		mhb.setOriginator(this.nodeId);

		// Raft Message to be added
		rm.setTerm(term);
		rm.setRaftAction(RaftAction.LEADER);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftmessage(rm.build());

		return mb.build();
	}

	// this method casts vote for a candidate
	private synchronized Management castVote() {
		RaftMessage.Builder rm = RaftMessage.newBuilder();
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security
		mhb.setOriginator(this.nodeId);

		// Raft Message to be added
		rm.setTerm(term);
		rm.setRaftAction(RaftAction.VOTE);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftmessage(rm.build());

		return mb.build();

	}

	int i = 0;

	// this method is used to send append notice to all the followers.
	public Management sendAppendNotice() {
			logger.info("Leader Node " + this.nodeId + " sending appendAction HB RPC's");
			RaftMessage.Builder rm = RaftMessage.newBuilder();
			MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
			mhb.setTime(System.currentTimeMillis());
			mhb.setSecurityCode(-999); // TODO add security
			mhb.setOriginator(this.nodeId);
			
			if(this.appendLogs){
				int tempLogIndex = this.lm.getLogIndex(); 
				rm.setPrevTerm(this.lm.getPrevLogTerm());
				rm.setLogIndex(tempLogIndex);
				rm.setPrevlogIndex(this.lm.getPrevLogIndex());
				
				for (Integer key : this.getLm().getEntries().keySet()) 
				{
					LogEntries.Builder le = LogEntries.newBuilder();
					String value = this.getLm().getEntries().get(key);
					le.setLogIndex(key);
					le.setLogData(value);
					rm.setEntries(key, le);
				}
				rm.setRaftAppendAction(RaftAppendAction.APPENDLOG);
				this.appendLogs = false;
			}
			else{
				rm.setRaftAppendAction(RaftAppendAction.APPENDHEARTBEAT);
			}
			
			rm.setTerm(term);
			rm.setRaftAction(RaftAction.APPEND);
			// Raft Message to be added
			Management.Builder mb = Management.newBuilder();
			mb.setHeader(mhb.build());
			mb.setRaftmessage(rm.build());
			return mb.build();
		//}
	}
	
	// this method is used to start the election process
	// if leader is found send leader message to all the followers
	private void startElection() {
		System.out.println("Timeout! Election declared by node " + getNodeId()
				+ "for term " + (term + 1));
		// Declare itself candidate, vote for self and Broadcast request for
		// votes To begin an election, a follower increments its current
		// term and transitions to candidate state. It then votes for
		// itself and issues RequestVote RPCs in parallel to each of
		// the other servers in the cluster.
		lastKnownBeat = System.currentTimeMillis();
		currentState = RState.Candidate;
		count = 1;
		term++;
		//If it is a single node then declare itself the winner else send REquest VOte message
		if (HeartbeatManager.getInstance().outgoingHB.size() == 0) {
			notify1(true, this.nodeId);
			count = 0;
			currentState = RState.Leader;
			leaderId = this.nodeId;
			logger.info(" Leader elected " + this.nodeId);
			ConnectionManager.broadcastAndFlush(sendLeaderMesssage());
		}

		else {
			ConnectionManager.broadcastAndFlush(sendRequestVoteNotice());
		}

	}

	// this method is used to build message for asking votes from other nodes in cluster.
	private Management sendRequestVoteNotice() {
		RaftMessage.Builder rm = RaftMessage.newBuilder();
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security
		mhb.setOriginator(this.nodeId);

		// Raft Message to be added
		rm.setTerm(term);
		rm.setRaftAction(RaftAction.REQUESTVOTE);
		rm.setLogIndex(this.getLm().getLogIndex());
		rm.setPrevlogIndex(this.getLm().getPrevLogIndex());
		rm.setPrevTerm(this.getLm().getPrevLogTerm());
		

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftmessage(rm.build());

		return mb.build();

	}
	
	
	// This class is a monitor class for raft consensus algorithm.
	public class RaftMonitor extends Thread {
		@Override
		// this method keeps a track if the leader exists or not. If exists it send append notice
		// It not then it will wait for timeout and start election
		public void run() {
			while (true) {
				try {
					Thread.sleep(11000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (currentState == RState.Leader)
					ConnectionManager.broadcastAndFlush(sendAppendNotice());
				else {
					boolean blnStartElection = RaftManager.getInstance()
							.assessCurrentState();
					if (blnStartElection) {
						long now = System.currentTimeMillis();
						if ((now - lastKnownBeat) > timeElection)
							startElection();
					}
				}
			}
		}
	}
	
	// notify the listener
	private void notify1(boolean success, Integer leader) {
		if (listener != null) {
			listener.concludeWith(success, leader);
		}
	}

	@SuppressWarnings("unused")
	// this function is used to update the current variable for a particular leader elected.
	private boolean updateCurrent(LeaderElection req) {
		boolean isNew = false;

		if (current == null) {
			current = new ElectionState();
			isNew = true;
		}

		current.electionID = req.getElectId();
		current.candidate = req.getCandidateId();
		current.desc = req.getDesc();
		current.maxDuration = req.getExpires();
		current.startedOn = System.currentTimeMillis();
		current.state = req.getAction();
		current.id = -1; // TODO me or sender?
		current.active = true;

		return isNew;
	}

	// this method is usedto create a election id.
	public Integer createElectionID() {
		return ElectionIDGenerator.nextID();
	}

	// this method return the winner if found else return null.
	public Integer getWinner() {
		if (current == null)
			return null;
		else if (current.state.getNumber() == ElectAction.DECLAREELECTION_VALUE)
			return current.candidate;
		else
			return null;
	}
	
	
	// getting and setting of the values
	
	public RState getCurrentState() {
		return currentState;
	}

	public void setCurrentState(RState currentState) {
		this.currentState = currentState;
	}

	public int getLeaderId() {
		return leaderId;
	}

	public void setLeaderId(int leaderId) {
		this.leaderId = leaderId;
	}

	public int getTimeElection() {
		return timeElection;
	}

	public void setTimeElection(int timeElection) {
		this.timeElection = timeElection;
	}
	
	public void setListener(ElectionListener listener) {
		this.listener = listener;
	}
	
	public Integer getElectionId() {
		if (current == null)
			return null;
		return current.electionID;
	}

	public Integer getNodeId() {
		return nodeId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	public synchronized void clear() {
		current = null;
	}

	public boolean isElectionInprogress() {
		return current != null;
	}
	
	public RaftMonitor getMonitor() {
		return monitor;
	}
	public LogMessage getLm() {
		return lm;
	}

	public void setLm(LogMessage lm) {
		this.lm = lm;
	}
	
	public boolean isAppendLogs() {
		return appendLogs;
	}

	public void setAppendLogs(boolean appendLogs) {
		this.appendLogs = appendLogs;
	}
}
