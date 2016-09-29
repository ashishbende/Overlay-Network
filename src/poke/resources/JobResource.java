/*
 * copyright 2012, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.resources;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import io.netty.channel.Channel;
import poke.Communication.CommunicationConnection;
import poke.comm.App.ClientMessage;
import poke.comm.App.Details;
import poke.comm.App.Header;
import poke.comm.App.Payload;
import poke.comm.App.PokeStatus;
import poke.comm.App.Request;
import poke.resources.DataResource.Course;
import poke.resources.DataResource.UserCourses;
import poke.resources.DataResource.Users;
import poke.comm.App.ClientMessage.Functionalities;
import poke.comm.App.ClientMessage.MessageType;
import poke.comm.App.Header.Routing;
import poke.comm.App.JobDesc;
import poke.comm.App.JobOperation;
import poke.comm.App.JobStatus;
import poke.comm.App.NameValueSet;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.managers.ConnectionManager;
import poke.server.managers.RaftManager;
import poke.server.managers.UpdateManager;
import poke.server.managers.ConnectionManager.connectionState;
import poke.server.resources.Resource;
import poke.server.resources.ResourceFactory;
import poke.server.resources.ResourceUtil;

public class JobResource implements Resource {
	
	ServerConf conf = ResourceFactory.cfg;

	@SuppressWarnings({ "unused", "static-access" })
	@Override
	public void process(Request request, Channel ch) {	
		
		String operation = request.getBody().getJobOp().getData().getNameSpace();
		if(operation.equals("intercluster"))
		{

			Request.Builder reply = Request.newBuilder();

			//Header
			reply.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), PokeStatus.SUCCESS, null));

			// payload
			Payload.Builder pb = Payload.newBuilder();
			JobStatus.Builder jsb = JobStatus.newBuilder();
			jsb.setJobId(request.getBody().getJobStatus().getJobId());
			jsb.setJobState(request.getBody().getJobStatus().getJobState());
			jsb.setStatus(PokeStatus.SUCCESS);
			
		   JobDesc.Builder jobDesc = JobDesc.newBuilder();
		   jobDesc.setNameSpace("listcourses");
		   jobDesc.setOwnerId(1234);
		   jobDesc.setJobId("100");
		   jobDesc.setStatus(JobDesc.JobCode.JOBRECEIVED);

		   System.out.println("Cluster Host and port!!!");

		   int port,j=0;
		   String host, course , description;
		   Request interclusterRequest = buildInterclusterRequest();

		   //for (Map.Entry<Integer, NodeDesc> cluster : conf.getAdjacentCluster().getAdjacentNodes().entrySet())
		   //{
		      port = 5574;
		      host = "10.0.0.5";
		      System.out.println("Host :" + host + "Port : " + port);

		      CommunicationConnection connection = new CommunicationConnection(host, port);
		      Request res = connection.send(interclusterRequest);

		      int count= res.getBody().getJobStatus().getData(0).getOptions().getNodeCount();
		      int i =0;

		      NameValueSet.Builder nv = NameValueSet.newBuilder();
		      nv.setNodeType(NameValueSet.NodeType.VALUE);
		      nv.setName("from cluster"+ host + " " + port);
		      NameValueSet.Builder nvc = NameValueSet.newBuilder();

		      for(int k=0 ; k<count ; k++)
		      {
		         course = res.getBody().getJobStatus().getData(0).getOptions().getNode(k).getName();
		         description = res.getBody().getJobStatus().getData(0).getOptions().getNode(k).getValue();
		         System.out.println("Course :" + course + "Description:" + description);

		         nvc.setNodeType(NameValueSet.NodeType.VALUE);
		         nvc.setName(course);
		         nvc.setValue(description);
		         nv.addNode(i++, nvc.build());
		      }

		      jobDesc.setOptions(nv.build());
		      jsb.addData(j++, jobDesc.build());
		   //}
		   pb.setJobStatus(jsb.build());
			reply.setBody(pb.build());
			ch.writeAndFlush(reply.build());
		}

		if(operation.equals("getInterclusterData"))
		{
			Request.Builder reply = Request.newBuilder();

			//Header
			reply.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), PokeStatus.SUCCESS, null));

			// payload
			Payload.Builder pb = Payload.newBuilder();
			JobStatus.Builder jsb = JobStatus.newBuilder();
			jsb.setJobId(request.getBody().getJobStatus().getJobId());
			jsb.setJobState(request.getBody().getJobStatus().getJobState());
			jsb.setStatus(PokeStatus.SUCCESS);
			
		   NameValueSet.Builder nvc = NameValueSet.newBuilder();
		   NameValueSet.Builder nv = NameValueSet.newBuilder();
		   nv.setNodeType(NameValueSet.NodeType.VALUE);


		   JobDesc.Builder jobDesc = JobDesc.newBuilder();
		   jobDesc.setNameSpace("Cluster Data of " + conf.getNodeId());
		   jobDesc.setOwnerId(conf.getNodeId());
		   jobDesc.setJobId("100");
		   jobDesc.setStatus(JobDesc.JobCode.JOBRECEIVED);

		   int i = 0;
		   Course course = null;
		   DataResource r = new DataResource();
		   HashMap<Integer, Course> entry = r.getAllCourseDetails();
		   Iterator<Integer> it = entry.keySet().iterator();
		   while(it.hasNext()){
			   int key = it.next();
			   Course c = entry.get(key);
			   nvc.setNodeType(NameValueSet.NodeType.VALUE);
			      nvc.setName(c.CourseName);
			      nvc.setValue(c.CourseDescription);
			      nv.addNode(i++, nvc.build());
		   }

		   jobDesc.setOptions(nv.build());
		   jsb.addData(0, jobDesc.build());
		   
		   pb.setJobStatus(jsb.build());
			reply.setBody(pb.build());
			ch.writeAndFlush(reply.build());
		}		
		else if (request.getBody().hasClusterMessage()) {
			// handle cluster message request and respond to the requester
			// handle inter-cluster communication
			ClientMessage clientMessage = request.getBody().getClusterMessage().getClientMessage();
			Functionalities value = clientMessage.getFunctionality();
			boolean reply = true;
			DataResource res = new DataResource();
			Users user = null;
			Course course = null;
			Details de = clientMessage.getDetails();
			
			switch(value.getNumber()){
			case Functionalities.GETSUSER_VALUE:
				user = res.getUserDetails(de.getUserId());
				if(user == null){
					reply = false;
				}
				break;
			case Functionalities.GETCOURSEDESCRIPTION_VALUE:
				course = res.getCourseDetails(de.getCourseId());
				if(course == null){
					reply = false;
				}
				break;
			}	
			
			if(reply){
				ClientMessage.Builder clBuilder = ClientMessage.newBuilder();
				String result = "";
				String lstUsercourses = "";

				if (user != null) {
					lstUsercourses = lstUsercourses + "[";
					lstUsercourses = lstUsercourses + "{ CourseId:";
					for (UserCourses uc : user.listUserCourse) {
						lstUsercourses = lstUsercourses + uc.CourseId + ",";
					}

					lstUsercourses = lstUsercourses + " }";
					lstUsercourses = lstUsercourses + "]";
					result = "{UserName: " + user.UserName + ", UserCourses: " + lstUsercourses + "}";
				} else if (course != null) {
					result = "{ CourseName: " + course.CourseName + ", CourseDescription: "
							+ course.CourseDescription + " }";
				}
				
				Header.Builder h = Header.newBuilder();
				h.setOriginator(request.getHeader().getOriginator());
				Header head = request.getHeader();
				// Now send back the reply
				Request.Builder r = Request.newBuilder();

				// Build Header
				h.setRoutingId(Routing.JOBS);
				h.setTime(System.currentTimeMillis());
				// NOt setting Poke Status, Reply Message, ROuting Path,
				// Name Value set
				// TO Node is the node to which this client will get
				// connected
				h.setToNode(head.getOriginator());
				clBuilder.setMessageType(MessageType.LEADERNODE);
				clBuilder.setClientId(clientMessage.getClientId());
				clBuilder.setSenderId(RaftManager.self);

				// Build Payload
				Payload.Builder p = Payload.newBuilder();
				p.setResult(result);
				// Not adding anything as it is just a register message
				p.setClientMessage(clBuilder);

				r.setBody(p);
				r.setHeader(h);
				Request req = r.build();
				int nodeId = clientMessage.getSenderId();
				System.out.println("CLUSTER MESSAGE - Node Id " + nodeId);
				ConnectionManager.sendToNode(req, nodeId);
			}
		} else {
			// handle different types of intra-cluster request.
			if(request.getBody().getClientMessage().getMessageType().getNumber() == MessageType.REPLICATIONLOCAL_VALUE){
				ClientMessage message = request.getBody().getClientMessage();
				String host = message.getHost();
				int port = message.getPort();
				int nodeid = message.getNodeid();
				String clusterid = message.getClusterid();
				UpdateManager.initializeServerConfig(host, port, nodeid, clusterid);
			}
			else if(request.getBody().getClientMessage().getMessageType().getNumber() == MessageType.REPLICATIONCLUSTER_VALUE){
				ClientMessage message = request.getBody().getClientMessage();
				String host = message.getHost();
				int port = message.getPort();
				int nodeid = message.getNodeid();
				String clusterid = message.getClusterid();
				UpdateManager.initializeServerConfig(host, port, nodeid, clusterid);
			}
			else if(request.getBody().getClientMessage().getMessageType().getNumber() == MessageType.CLIENT_REQUEST_VALUE){
				boolean execute = true;
				// handle client request
				// forward it to leader if no records are found
				// if record found reply to the user.
				System.out.println("Client application message received");
				ClientMessage clientMessage = request.getBody().getClientMessage();
				int clientId = request.getBody().getClientMessage().getClientId();
				Header.Builder h = Header.newBuilder();
				h.setOriginator(RaftManager.self);
				DataResource res = new DataResource();
				Details de = clientMessage.getDetails();
				Users user = null;
				Course course = null;
				ClientMessage.Builder clBuilder = ClientMessage.newBuilder();
				ConnectionManager.addConnection(clientId, ch, connectionState.CLIENTAPP);

				switch (clientMessage.getFunctionality().getNumber()) {
				case Functionalities.GETSUSER_VALUE:
					user = res.getUserDetails(de.getUserId());
					clBuilder.setFunctionality(Functionalities.GETSUSER);
					if (user == null) {
						System.out.println("user details found null");
						execute = false;
						System.out.println("LEADER FROM RAFT IS " + RaftManager.getInstance().whoIsTheLeader());
						System.out.println("LEADER FROM MANAGER IS " + RaftManager.getInstance().self);
						if (RaftManager.getInstance().whoIsTheLeader().intValue() == RaftManager.getInstance().self) {							
							RaftManager.getInstance().sendMessageToOtherCluster(request, RaftManager.self);
						}
						else{
							sendMessageToLeader(request);
						}
					}
					break;
				case Functionalities.GETCOURSEDESCRIPTION_VALUE:
					course = res.getCourseDetails(de.getCourseId());
					clBuilder.setFunctionality(Functionalities.GETCOURSEDESCRIPTION);
					if (course == null) {
						execute = false;
						// if leader, broadcast message to all clusters
						if (RaftManager.getInstance().whoIsTheLeader().intValue() == RaftManager.getInstance().self) {
							System.out.println("received null value for user.");
							RaftManager.getInstance().sendMessageToOtherCluster(request, RaftManager.self);
						}
						else{
							// if follower send message to leader
							sendMessageToLeader(request);
						}
					}
					break;
				default:
					break;
				}

				if (execute) {
					String result = "";
					String lstUsercourses = "";

					if (user != null) {
						lstUsercourses = lstUsercourses + "[";
						lstUsercourses = lstUsercourses + "{ CourseId:";
						for (UserCourses uc : user.listUserCourse) {
							lstUsercourses = lstUsercourses + uc.CourseId + ",";
						}

						lstUsercourses = lstUsercourses + " }";
						lstUsercourses = lstUsercourses + "]";
						result = "{UserName: " + user.UserName + ", UserCourses: " + lstUsercourses + "}";
					} else if (course != null) {
						result = "{ CourseName: " + course.CourseName + ", CourseDescription: "
								+ course.CourseDescription + " }";
					}
					

					Header head = request.getHeader();
					// Now send back the reply
					Request.Builder r = Request.newBuilder();

					// Build Header
					h.setRoutingId(Routing.JOBS);
					h.setTime(System.currentTimeMillis());
					// NOt setting Poke Status, Reply Message, ROuting Path,
					// Name Value set
					// TO Node is the node to which this client will get
					// connected
					h.setToNode(head.getOriginator());
					clBuilder.setMessageType(poke.comm.App.ClientMessage.MessageType.SUCCESS);

					// Build Payload
					Payload.Builder p = Payload.newBuilder();
					p.setResult(result);
					// Not adding anything as it is just a register message
					p.setClientMessage(clBuilder);

					r.setBody(p);
					r.setHeader(h);
					Request req = r.build();
					ConnectionManager.sendToClient(req, clientId);
				}
			}
			else if(request.getBody().getClientMessage().getMessageType().getNumber() == MessageType.OTHERCLUSTER_VALUE){
				// This message is received by leader of the cluster for forwarding it to other clusters.
				System.out.println("rvalue of node is. " + RaftManager.self);
				// add a channel between the originator and the leader.
				ConnectionManager.addConnection(request.getHeader().getOriginator(), ch, connectionState.LOCALSERVERAPP);
				RaftManager.getInstance().sendMessageToOtherCluster(request, request.getHeader().getOriginator());
			}
			else if(request.getBody().getClientMessage().getMessageType().getNumber() == MessageType.LEADERNODE_VALUE){
				// message from other clusters for forwarding it to originator
				// this will be only handled by leader
				int nodeid = request.getHeader().getOriginator();
								
				ClientMessage msg = request.getBody().getClientMessage();
				Header head = request.getHeader();
				Request.Builder r = Request.newBuilder();

				// Build Header
				Header.Builder h = Header.newBuilder();
				h.setRoutingId(Routing.JOBS);
				h.setOriginator(head.getOriginator());
				h.setTime(System.currentTimeMillis());
				
				ClientMessage.Builder clBuilder = ClientMessage.newBuilder();
				clBuilder.setMessageType(MessageType.CLIENTRESPONSE);
				clBuilder.setFunctionality(msg.getFunctionality());
				clBuilder.setClientId(msg.getClientId());
				
				// Build Payload
				Payload.Builder p = Payload.newBuilder();
				p.setResult(request.getBody().getResult());
				// Not adding anything as it is just a register message
				p.setClientMessage(clBuilder);

				r.setBody(p);
				r.setHeader(h);
				Request req = r.build();
				if(RaftManager.self == nodeid){
					ConnectionManager.sendToClient(request, msg.getClientId());
				}
				else{
					ConnectionManager.sendToLocalNode(request, nodeid);
				}
			}
			else if(request.getBody().getClientMessage().getMessageType().getNumber() == MessageType.CLIENTRESPONSE_VALUE){
				System.out.println("GOT RESPONSE FOR CLIENT");
				ClientMessage msg = request.getBody().getClientMessage();				
				int clientId = msg.getClientId();
				Header.Builder h = Header.newBuilder();
				h.setOriginator(RaftManager.getInstance().self);
				Header head = request.getHeader();
				// Now send back the reply
				Request.Builder r = Request.newBuilder();

				// Build Header
				h.setRoutingId(Routing.JOBS);
				h.setTime(System.currentTimeMillis());
				// NOt setting Poke Status, Reply Message, ROuting Path,
				// Name Value set
				// TO Node is the node to which this client will get
				// connected
				h.setToNode(head.getOriginator());
				ClientMessage.Builder clBuilder = ClientMessage.newBuilder();
				clBuilder.setMessageType(poke.comm.App.ClientMessage.MessageType.SUCCESS);

				// Build Payload
				Payload.Builder p = Payload.newBuilder();
				p.setResult(request.getBody().getResult());
				// Not adding anything as it is just a register message
				p.setClientMessage(clBuilder);

				r.setBody(p);
				r.setHeader(h);
				Request req = r.build();
				ConnectionManager.sendToClient(req, clientId);				
			}
		}
	}

	public void sendMessageToLeader(Request request) {
		// building message to send the request to leader of the cluster.
		Header reqHeader = request.getHeader();
		ClientMessage reqClientMsg = request.getBody().getClientMessage();

		ClientMessage.Builder cBuilder = ClientMessage.newBuilder();
		cBuilder.setMsgId(reqClientMsg.getMsgId());

		if (reqClientMsg.hasDetails()) {
			Details.Builder dBuilder = Details.newBuilder();
			dBuilder.setCourseId(reqClientMsg.getDetails().getCourseId());
			dBuilder.setUserId(reqClientMsg.getDetails().getUserId());
			dBuilder.setUsername(reqClientMsg.getDetails().getUsername());
			dBuilder.setCourseName(reqClientMsg.getDetails().getCourseName());
			cBuilder.setDetails(dBuilder);

			cBuilder.setFunctionality(reqClientMsg.getFunctionality());
			cBuilder.setMessageType(MessageType.OTHERCLUSTER);
			cBuilder.setRequestType(reqClientMsg.getRequestType());
			cBuilder.setClientId(reqClientMsg.getClientId());
		}

		Header.Builder h = Header.newBuilder();

		h.setRoutingId(Routing.JOBS);
		h.setTag(reqHeader.getTag());
		h.setOriginator(RaftManager.self);
		h.setTime(System.currentTimeMillis());
		h.setToNode(reqHeader.getToNode());

		// add client msg to body
		Payload.Builder payload = Payload.newBuilder();
		payload.setClientMessage(cBuilder);

		// add body to request
		Request.Builder req = Request.newBuilder();
		req.setBody(payload);
		req.setHeader(h);
		Request r = req.build();
		ConnectionManager.sendToLocalNode(r, RaftManager.getInstance().whoIsTheLeader());
	}
	
	private Request buildInterclusterRequest()
	{
	   Request.Builder forward = Request.newBuilder();

	   Header.Builder header = Header.newBuilder();
	   header.setOriginator(conf.getNodeId());
	   header.setRoutingId(Header.Routing.JOBS);
	   forward.setHeader(header.build());

	   Payload.Builder forwardpb = Payload.newBuilder();

	   JobStatus.Builder forwardjsb = JobStatus.newBuilder();
	   forwardjsb.setJobId("5555");
	   forwardjsb.setJobState(JobDesc.JobCode.JOBQUEUED);
	   forwardjsb.setStatus(PokeStatus.SUCCESS);

	   JobOperation.Builder forwardJobOp = JobOperation.newBuilder();
	   forwardJobOp.setJobId("5555");
	   forwardJobOp.setAction(JobOperation.JobAction.LISTJOBS);

	   JobDesc.Builder forwardData = JobDesc.newBuilder();
	   forwardData.setNameSpace("getInterclusterData");
	   forwardData.setOwnerId(5555);
	   forwardData.setJobId("5555");
	   forwardData.setStatus(JobDesc.JobCode.JOBQUEUED);
	   forwardJobOp.setData(forwardData.build());

	   forwardpb.setJobOp(forwardJobOp.build());
	   forwardpb.setJobStatus(forwardjsb.build());
	   forward.setBody(forwardpb.build());
	   return forward.build();
	}
}
