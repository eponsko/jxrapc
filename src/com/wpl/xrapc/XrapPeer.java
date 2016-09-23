package com.wpl.xrapc;

import com.googlecode.concurrenttrees.common.Iterables;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import com.wpl.xrapc.cli.TestResource;
import com.wpl.xrapc.cli.XrapResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import zmq.ZError;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A general purpose client for the XRAP protocol.
 * <p/>
 * For details, see http://rfc.zeromq.org/spec:40
 *
 * @author tomq
 */


public class XrapPeer implements Runnable {

    static int threadnum = 1;
    RadixTree<XrapResource> routeTrie;
    private int port;
    private String host;
    private ZMQ.Socket sockDealer, sockPush, sockSub;
    private ZMQ.Socket sockRouter, sockPull, sockPub;
    private long receiveTimeout = 30;
    private TimeUnit receiveTimeoutUnit = TimeUnit.SECONDS;
    private Map<Integer, com.wpl.xrapc.XrapReply> responseCache = new ConcurrentHashMap<Integer, XrapReply>();
    private Lock lock = new ReentrantLock();
    private boolean isServer;
    private Logger log;
    private ZMQ.Socket signal;
    private ZContext ctx;
    private int signalId, routerId, dealerId, pubId, subId, pushId, pullId;
    private boolean terminate = false;

    /**
     * Creates a new XrapClient object using a newly created ZMQ context.
     *
     * @param host The endpoint to connect to. This should be of the form
     *             tcp://hostname/port (see http://api.zeromq.org/4-0:zmq-tcp)
     */
    public XrapPeer(String host, int port, boolean isServer, ZContext context) {
        this.log = LoggerFactory.getLogger(XrapPeer.class);

        this.host = host;
        this.port = port;
        this.ctx = context;
        this.isServer = isServer;
        routeTrie = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
        XrapResource handler = new XrapResource();
        addHandler(handler);
    }

    public void addHandler(XrapResource handler) {
        log.info("Registering route " + handler.getRoute()+ " with handler : " + handler.getClass() );
        routeTrie.put(handler.getRoute(), handler);

    }

    public void terminate() {
        terminate = true;
    }

    /**
     * Sets the timeout for the request, in seconds.
     * This is 30 by default.
     *
     * @param seconds The new timeout, in seconds
     */
    public void setTimeout(int seconds) {
        this.receiveTimeout = seconds;
        this.receiveTimeoutUnit = TimeUnit.SECONDS;
    }

    /**
     * Sets the timeout for the request, in seconds.
     * This is 30 by default.
     *
     * @param count The new timeout
     * @param units time units
     */
    public void setTimeout(long count, TimeUnit units) {
        this.receiveTimeout = count;
        this.receiveTimeoutUnit = units;
    }

    /**
     * Sends the given request, and blocks waiting for the reply.
     *
     * @param request An XrapRequest object defining the request to make.
     * @return An XrapReply representing the reply sent from the server.
     * @throws XrapException if there is an issue with the XRAP protocol
     *                       such as a communication error. Note that the returned XrapReply
     *                       will describe errors returned by the server, these are not thrown
     *                       as exceptions.
     */
    public XrapReply send(XrapRequest request) throws XrapException, InterruptedException {
        sendOnly(request);
        XrapReply response = getResponse(request, receiveTimeout, receiveTimeoutUnit);
        if (response == null) throw new XrapException("Timeout");
        return response;
    }

    private void sendReply(ZMQ.Socket sock, XrapReply rep) throws XrapException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            rep.buildReply(dos);
        } catch (IOException ex) {
            // shouldn't occur when writing to a ByteArrayOutputStream?
        }
        try {
            lock.lock();
            log.info("socket is: " + sock);
            ByteBuffer bb = rep.getRouteid();
            if(bb != null) {
                sock.send(bb.array(), ZMQ.SNDMORE);
                sock.send(new byte[0], ZMQ.SNDMORE);
            } else {
                sock.send(new byte[0], ZMQ.SNDMORE);
            }
            sock.send(baos.toByteArray(), 0);
            log.info("sendReply, size: " + baos.size() );
        } finally {
            lock.unlock();
        }
    }

    private void sendOnly(XrapRequest request) throws XrapException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            request.buildRequest(dos);
        } catch (IOException ex) {
            // shouldn't occur when writing to a ByteArrayOutputStream?
        }
        if (!isServer) {
            try {
                lock.lock();
                log.info("sockDealer is: " + sockDealer);
                sockDealer.send(new byte[0], ZMQ.SNDMORE);
                sockDealer.send(baos.toByteArray(), 0);
                log.info("sendOnly, size: " + baos.size() );
            } finally {
                lock.unlock();
            }
        } else if (!isServer) {
            try {
                lock.lock();
                log.info("sockRouter is: " + sockRouter);
                sockRouter.send(new byte[0], ZMQ.SNDMORE);
                sockRouter.send(baos.toByteArray(), 0);
            } finally {
                lock.unlock();

            }
        }
    }

    private XrapReply getResponse(XrapRequest request, long timeout, TimeUnit unit) throws XrapException, InterruptedException {
        XrapReply reply;
        log.warn("getResponse called, timeout: " + timeout + " " + unit );
        // There are two timeouts. We have to ensure that we return in a time
        // consistent with the timeout passed as argument. We first have to acquire the
        // lock. Another thread may have the lock, and may be waiting on a longer
        // timeout, waiting on the socket.

        long timeoutms = unit.toMillis(timeout);
        while (timeoutms > 0) {

            // First see whether the response has already been received, either
            // by us previously, or by another thread that might also be waiting.
            if ((reply = responseCache.remove(request.getRequestId())) != null) {
                log.warn("getResponse: response was in cache!\n");
                return reply;
            } else {
                log.warn("getResponse: not in cache\n");
            }
            synchronized (responseCache) {
                long loopStart = new java.util.Date().getTime();
                responseCache.wait(timeoutms);
                long loopStop = new java.util.Date().getTime();
                timeoutms -= (loopStop-loopStart);
            }
        }
        log.warn("getResponse did not receive.. ");
        return null;
        /*
        byte[] responseBytes;
        if (!lock.tryLock(timeoutms, TimeUnit.MILLISECONDS)) {
            log.warn("Could not lock!\n");
            return null;
        }
        try {
            log.warn("waiting in recv for data..");
            sockDealer.setReceiveTimeOut((int) Math.min(timeoutms, Integer.MAX_VALUE));
            responseBytes = sockDealer.recv();

            timeoutms -= new java.util.Date().getTime() - loopStart;
            if (responseBytes == null) {
                // Timed out, or error?
                // Not sure how we tell the difference.
                continue;
            }
            log.warn("Got data from dealer!, size: " + responseBytes.length);
                // Depending on whether a REQ or DEALER is used, we might get an
                // empty delimiter frame.
                if (responseBytes.length == 0) {
                    log.warn("Got an empty frame, waiting for more..\n");
                    responseBytes = sockDealer.recv();
                }
            } finally {
                lock.unlock();
            }

            reply = request.parseResponse(responseBytes);
            if (reply.getRequestId() == request.getRequestId())
                return reply;
            responseCache.put(request.getRequestId(), reply);
        }
        return null;
        */
    }

    /**
     * Makes an asynchronous request.
     *
     * @param request
     * @return A {@link java.util.concurrent.Future} object through which the result can be acquired.
     * If an error occurs receiving the reply, then an {@link java.util.concurrent.ExecutionException} can be thrown
     * wrapping the underlying {@link XrapException}.
     * @throws XrapException
     */
    public Future<XrapReply> sendAsync(XrapRequest request) throws XrapException {
        sendOnly(request);
        return new FutureReply(request);
    }

    private XrapReply getResponse(XrapRequest request) throws XrapException, InterruptedException {
        return getResponse(request, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        Thread.currentThread().setName(String.format("XrapPeer-%d", threadnum));

        if (isServer) {
            log.info("Creating XRAP peer as server, binding to: " + String.format("tcp://%s:%d", host, port));
            sockRouter = ctx.createSocket(ZMQ.ROUTER);
            sockRouter.bind(String.format("tcp://%s:%d", host, port));
            sockPush = ctx.createSocket(ZMQ.PUSH);
            sockPush.bind(String.format("tcp://%s:%d", host, port + 1));
            sockPub = ctx.createSocket(ZMQ.PUB);
            sockPub.bind(String.format("tcp://%s:%d", host, port + 2));
            log.info("Created sockRouter: " + sockRouter);
            log.info("Created sockPush: " + sockPush);
            log.info("Created sockPub: " + sockPub);
        } else {
            log.info("Creating XRAP peer as client, connecting to: " + String.format("tcp://%s:%d", host, port));
            // If we're client (i.e. Net2Plan), connect to
            sockDealer = ctx.createSocket(ZMQ.DEALER);
            sockDealer.connect(String.format("tcp://%s:%d", host, port));
            sockPull = ctx.createSocket(ZMQ.PULL);
            sockPull.connect(String.format("tcp://%s:%d", host, port + 1));
            sockSub = ctx.createSocket(ZMQ.SUB);
            sockSub.connect(String.format("tcp://%s:%d", host, port + 2));
            log.info("Created sockDealer: " + sockDealer);
            log.info("Created sockPull: " + sockPull);
            log.info("Created sockSub: " + sockSub);
        }
        signal = ctx.createSocket(ZMQ.PAIR);
        signal.connect("tcp://127.0.0.1:9999");
        log.info("Connected signal: " + signal);
        // Wait for new messages, receive them, and process
        while (!Thread.currentThread().isInterrupted() && !terminate) {
            //log.info("polling");
            ZMQ.Poller items = new ZMQ.Poller(4);
            if (isServer) {
                signalId = items.register(signal, ZMQ.Poller.POLLIN);
                routerId = items.register(sockRouter, ZMQ.Poller.POLLIN);
                pubId = items.register(sockPub, ZMQ.Poller.POLLIN);
                pushId = items.register(sockPush, ZMQ.Poller.POLLIN);
            } else {
                signalId = items.register(signal, ZMQ.Poller.POLLIN);
                dealerId = items.register(sockDealer, ZMQ.Poller.POLLIN);
                subId = items.register(sockSub, ZMQ.Poller.POLLIN);
                pullId = items.register(sockPull, ZMQ.Poller.POLLIN);
            }
            try {
                if (items.poll(1000) == -1) {
                    log.warn("items.poll() returned -1\n");
                    break;

                } else if (isServer) {
                    if (items.pollin(routerId)) {
                        log.warn("Router socket got message\n");
                        ByteBuffer routeId = ByteBuffer.wrap(sockRouter.recv());
                        byte[] empty = sockRouter.recv();
                        byte[] responseBytes = sockRouter.recv();
                        if (sockRouter.hasReceiveMore()) {
                            log.warn("sockRouter hasRecieveMore\n");
                        }
                        log.warn("Recieved data, len: " + responseBytes.length );
                        if (responseBytes.length > 0) {
                            XrapMessage msg = XrapMessage.parseRequest(ByteBuffer.wrap(responseBytes));
                            msg.setRouteid(routeId);
                            if (msg instanceof XrapReply) {
                                log.warn("Server got a XrapReply! Putting in the reply buffer..\n");
                                responseCache.put(msg.getRequestId(),(XrapReply)msg);
                                synchronized (responseCache){
                                    responseCache.notify();
                                }
                            } else {
                                log.info("Got a request: \n" + msg.toString());
                                XrapReply rep = handleRequest(msg);
                                sendReply(sockRouter, rep);
                            }
                        }
                    }
                    if (items.pollin(pubId))
                        log.warn("Pub socket got message\n");
                    if (items.pollin(pushId))
                        log.warn("Push socket got message\n");
                    if (items.pollin(signalId)) {
                        log.warn("Got signal, aborting!\n");
                        //Thread.currentThread().interrupt();
                        terminate = true;
                        ctx.destroy();
                    }
                } else if (!isServer) {
                    if (items.pollin(dealerId)) {
                        log.warn("Dealer socket got message\n");
                        byte[] empty = sockDealer.recv();
                        log.warn("empty frame: ");
                        byte[] responseBytes = sockDealer.recv();
                        if(responseBytes == null){
                            log.warn("Got null from sockdealer.recv()!");
                            continue;
                        }
                        if (sockDealer.hasReceiveMore()) {
                            log.warn("sockRouter hasRecieveMore\n");
                        }
                        log.warn("Recieved data, len: " + responseBytes.length );
                        if (responseBytes.length > 0) {
                            XrapMessage msg = XrapMessage.parseRequest(ByteBuffer.wrap(responseBytes));
                            if (msg instanceof XrapReply) {
                                log.warn("Client got a XrapReply! Putting in the reply buffer..\n");

                                responseCache.put(msg.getRequestId(), (XrapReply) msg);
                                synchronized (responseCache){
                                    responseCache.notify();
                                }
                            } else {
                                log.info("Client got a request: \n" + msg.toString());
                                XrapReply rep = handleRequest(msg);
                                sendReply(sockDealer, rep);
                            }
                        }
                    }
                    if (items.pollin(subId))
                        log.warn("Sub socket got message\n");
                    if (items.pollin(pullId))
                        log.warn("Pull socket got message\n");
                    if (items.pollin(signalId)) {
                        log.warn("Got signal, aborting!\n");
                        ctx.destroy();
                        //Thread.currentThread().interrupt();
                        terminate = true;
                    }
                }
            } catch (ZError.IOException e) {
                log.error("items.poll() caught exception: " + e);
                ctx.destroy();
                Thread.currentThread().interrupt();
            } catch (XrapException e) {
                e.printStackTrace();
            }
        }
        log.info("XrapPeer: returning from run()\n");
        log.info("XrapPeer: context : " + ctx );
        for (ZMQ.Socket s : ctx.getSockets()) {
            log.info("Socket: " + s );
        }

    }

    private XrapReply handleRequest(XrapMessage msg) {
        XrapReply rep = new XrapErrorReply(msg.getRequestId(), Constants.BadRequest_400, "Malformed message");
        if (msg instanceof XrapReply) {
            log.info("handleRequest: got REPLY message: \n" + msg.toString() + "\n");
        } else {
            XrapResource handle = findHandler(msg);
            if (msg instanceof XrapGetRequest) {
                rep = handle.GET((XrapGetRequest) msg);
            } else if (msg instanceof XrapPutRequest) {
                rep = handle.PUT((XrapPutRequest) msg);
            } else if (msg instanceof XrapPostRequest) {
                rep = handle.POST((XrapPostRequest) msg);
            } else if (msg instanceof XrapDeleteRequest) {
                rep = handle.DELETE((XrapDeleteRequest) msg);
            }
        }
        log.info("handleRequest, replying with:\n" + rep.toString());
        return rep;
    }

    XrapResource trieLookup(String location){
        // first check for perfect match
        XrapResource res = routeTrie.getValueForExactKey(location);
        if (res != null)
            return res;
        // Check for closest match, return first
        for (XrapResource r : routeTrie.getValuesForClosestKeys(location)){
            return r;
        }
        // finally, look for default
        log.warn("No handler found for \"" + location + "\", returning default");
        res = routeTrie.getValueForExactKey("/");
        return res;
    }

    XrapResource findHandler(XrapMessage msg) {
        XrapResource handler = null;
        String location;
        if (msg instanceof XrapRequest) {
            location = ((XrapRequest) msg).getResource();
            log.info("Looking for handler for route: " + location);
            handler = trieLookup(location);
        }
        log.info("Found handler: " + handler.getClass());
        return handler;
    }

    private class FutureReply implements Future<XrapReply> {
        private XrapRequest request;
        private XrapReply response;
        private XrapException ex;

        public FutureReply(XrapRequest request) {
            this.request = request;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            // Cancellation not supported.
            return false;
        }

        @Override
        public boolean isCancelled() {
            // Cancellation not supported.
            return false;
        }

        @Override
        public boolean isDone() {
            try {
                if (response == null)
                    response = getResponse(request, 0, TimeUnit.SECONDS);
            } catch (XrapException ex) {
                this.ex = ex;
            } catch (InterruptedException ex) {
            }
            return response != null;
        }

        @Override
        public XrapReply get() throws InterruptedException, ExecutionException {
            try {
                if (ex != null) throw ex;
                if (response == null)
                    response = getResponse(request);
                return response;
            } catch (XrapException ex) {
                throw new ExecutionException(ex);
            }
        }

        @Override
        public XrapReply get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            try {
                if (ex != null) throw ex;
                if (response == null)
                    response = getResponse(request, timeout, unit);
                if (response == null)
                    throw new TimeoutException();
                return response;
            } catch (XrapException ex) {
                throw new ExecutionException(ex);
            }
        }

    }
}
