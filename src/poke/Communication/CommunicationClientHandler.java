package poke.Communication;

import com.google.protobuf.GeneratedMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poke.Communication.CommunicationListener;
import poke.comm.App;
import poke.comm.App.Request;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class CommunicationClientHandler extends SimpleChannelInboundHandler<Request>{
	protected static Logger logger = LoggerFactory.getLogger("connect");
	protected ConcurrentMap<String, CommunicationListener> listeners = new ConcurrentHashMap<String, CommunicationListener>();
	private volatile Channel channel;
	//private GeneratedMessage firstMessage;

	private final BlockingQueue<Request> answer = new LinkedBlockingQueue<Request>();

		
	public CommunicationClientHandler() {
		//firstMessage = msg;
		 super(false);
	}

	/**
	 * messages pass through this method. We use a blackbox design as much as
	 * possible to ensure we can replace the underlining communication without
	 * affecting behavior.
	 * 
	 * @param msg
	 * @return
	 */
	/*
	@Override 
  public void channelActive(ChannelHandlerContext ctx) {
	         // Send the first message if this handler is a client-side handler.
		System.out.println(" Came to channelActive");
		ChannelFuture cf = ctx.writeAndFlush(firstMessage);
		if (cf.isDone() && !cf.isSuccess()) {
			logger.error("@@@@@@@@@@@@@@@@@@@@@2failed to poke!");
			System.out.println("channelActive failed to poke!");
			System.out.println("cf.isDone() "+cf.isDone());
			System.out.println("cf.isSuccess() "+cf.isSuccess());
			//return false;
		}
	      }
	*/
	public Request send(Request msg) {


		// TODO a queue is needed to prevent overloading of the socket
		// connection. For the demonstration, we don't need it
		//channel = ch;
		System.out.println("Reached Send@@@@");
		ChannelFuture cf= channel.writeAndFlush(msg);   //writeAndFlush(msg, true);
		//channel.flush();

		Request messageFromLeader;
		boolean interrupted = false;
		for (;;) {
			try {
				messageFromLeader = answer.take();
				System.out.println("got message from leader !!");
				break;
			} catch (InterruptedException ignore) {
				interrupted = true;
			}
		}

		if (interrupted) {
			Thread.currentThread().interrupt();
		}
		System.out.println("Returning from sedn request!!");

		if (cf.isDone() && !cf.isSuccess()) {
			System.out.println(" Comminication Client hanler - Retry !!! ");
		}

		return messageFromLeader;
	}

	/**
	 * Notification registration. Classes/Applications receiving information
	 * will register their interest in receiving content.
	 * 
	 * Note: Notification is serial, FIFO like. If multiple listeners are
	 * present, the data (message) is passed to the listener as a mutable
	 * object.
	 * 
	 * @param listener
	 */
	public void addListener(CommunicationListener listener) {
		if (listener == null)
			return;

		listeners.putIfAbsent(listener.getListenerID(), listener);
	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Request msg) throws Exception {
		System.out.println("Received Msg@@@@@@@");
		answer.add(msg);
		for (String id : listeners.keySet()) {
			CommunicationListener cl = listeners.get(id);


			// TODO this may need to be delegated to a thread pool to allow
			// async processing of replies
			cl.onMessage(msg);
		}
	}
	

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) {
        channel = ctx.channel();
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
    
   

}
