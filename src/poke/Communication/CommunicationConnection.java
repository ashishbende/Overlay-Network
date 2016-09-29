/*
 * copyright 2014, gash
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
package poke.Communication;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.Communication.CommunicationInitializer;
import poke.Communication.CommunicationListener;
import poke.Communication.CommunicationClientHandler;

import com.google.protobuf.GeneratedMessage;
import poke.comm.App;

/**
 * provides an abstraction of the communication to the remote server. This could
 * be a public (request) or a internal (management) request.
 * 
 * Note you cannot use the same instance for both management and request based
 * messaging.
 * 
 * @author gash
 * 
 */
public class CommunicationConnection {
	protected static Logger logger = LoggerFactory.getLogger("connect");

	private String host;
	private int port;
	private ChannelFuture channel; // do not use directly call

	private EventLoopGroup group;
	private CommunicationClientHandler handler;


	/**
	 * Create a connection instance to this host/port. On construction the
	 * connection is attempted.
	 *
	 * @param host
	 * @param port
	 */
	public CommunicationConnection(String host, int port) {
		this.host = host;
		this.port = port;

		init();
	}

	public CommunicationConnection() {

	}

	/**
	 * release all resources
	 */
	public void release() {
		group.shutdownGracefully();
	}



	private void init() {
		// the queue to support client-side surging
		//outbound = new LinkedBlockingDeque<GeneratedMessage>();

		EventLoopGroup group = new NioEventLoopGroup();
		try {
			Bootstrap b = new Bootstrap();
			b.group(group)
					.channel(NioSocketChannel.class)
					.handler(new CommunicationInitializer());
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);


			// Make a new connection.
			channel = b.connect(host, port).sync();

			// Get the handler instance to initiate the request.
			handler = channel.channel().pipeline().get(CommunicationClientHandler.class);
			//ch.close();
			System.out.println("Connection established!!");
			ClientClosedListener ccl = new ClientClosedListener(this);
			channel.channel().closeFuture().addListener(ccl);
		} catch (Exception ex) {
			System.out.println("failed to initialize the client connection "+ ex);

		}


	}

	/**
	 * create connection to remote server
	 * 
	 * @return
	 */

	public App.Request send(App.Request msg)
	{
		return handler.send(msg);
	}

	protected Channel connect() {
		// Start the connection attempt.
		if (channel == null) {
			init();
		}

		if (channel.isDone() && channel.isSuccess())
			return channel.channel();
		else
			throw new RuntimeException("Not able to establish connection to server");
	}


	public static class ClientClosedListener implements ChannelFutureListener {
		CommunicationConnection cc;

		public ClientClosedListener(CommunicationConnection cc) {
			this.cc = cc;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			// we lost the connection or have shutdown.
			System.out.println("Communication Listener : Closing the future");
			future.channel().close();
			// @TODO if lost, try to re-establish the connection

		}
	}
}
