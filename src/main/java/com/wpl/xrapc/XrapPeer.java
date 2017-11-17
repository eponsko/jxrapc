package com.wpl.xrapc;

import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.googlecode.concurrenttrees.common.KeyValuePair;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import org.reflections.Reflections;
import org.reflections.scanners.*;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;



/**
 * A general purpose client for the XRAP protocol.
 * <p/>
 * For details, see http://rfc.zeromq.org/spec:40
 *
 * @author tomq
 */


public class XrapPeer implements Runnable {
    private static int threadnum = 1;
    private final Map<Integer, com.wpl.xrapc.XrapReply> responseCache = new ConcurrentHashMap<Integer, XrapReply>();

    private RadixTree<XrapResourceService> routeTrie;
    private int port;
    private String host;
    private ZMQ.Socket sockDealer, sockPush, sockSub;
    private ZMQ.Socket sockRouter, sockPull, sockPub;
    private ZMQ.Socket signal;
    private ZContext ctx;
    private boolean ready = false;
    private long receiveTimeout = 30;
    private TimeUnit receiveTimeoutUnit = TimeUnit.SECONDS;
    private Lock lock = new ReentrantLock();
    private boolean isServer;
    private Logger log;
    private int signalId, routerId, dealerId, pubId, subId, pushId, pullId;
    private boolean terminate = false;
    private RadixTree<IXrap> registeredClasses;
    private HashMap<Method, Pattern> urlPatterns;
    enum callType {JAXRS, RESOURCE};
    callType type = callType.RESOURCE;
    JSON json = new JSON();
    /**
     * Creates a new XrapClient object using a newly created ZMQ context.
     *
     * @param host The endpoint to connect to. This should be of the form
     *             tcp://hostname/port (see http://api.zeromq.org/4-0:zmq-tcp)
     */
    public XrapPeer(String host, int port, boolean isServer) {
        this.log = LoggerFactory.getLogger(XrapPeer.class);
        this.host = host;
        this.port = port;
        this.isServer = isServer;
        routeTrie = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
        registeredClasses = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
        urlPatterns = new HashMap<>();
        XrapResource handler = new XrapResource();
        addHandler(handler);
        type = callType.RESOURCE;
    }

    public void addHandler(XrapResourceService handler) {
        log.info("Registering route " + handler.getRoute() + " with handler : " + handler.getClass());
        routeTrie.put(handler.getRoute(), handler);
        type = callType.RESOURCE;
    }


    private synchronized void setReady() {
        ready = true;
        notifyAll();
    }

    public synchronized void waitUntilReady() {
        log.warn("waitUntilReady called..");
        try {
            while (!ready) {
                wait();
            }
        } catch (InterruptedException e) {
            log.error(e.toString());
        }
        log.warn("waitUntilReady done!");
    }


    public void delHandler(XrapResourceService handler) {
        log.info("Registering route " + handler.getRoute() + " with handler : " + handler.getClass());
        routeTrie.remove(handler.getRoute());
    }

    public void terminate() {
        terminate = true;
        //ZMQ.Socket sendsig = ctx.socket(ZMQ.REQ);
        ZMQ.Socket sendsig = ctx.createSocket(ZMQ.REQ);
        sendsig.connect("inproc://signal");
        sendsig.send("shutdown");
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
        if (response == null) {
            throw new XrapException("Timeout");
        }
        return response;
    }

    public XrapReply send(ByteBuffer address, XrapRequest request) throws XrapException, InterruptedException {
        sendOnly(address, request);
        XrapReply response = getResponse(request, receiveTimeout, receiveTimeoutUnit);
        if (response == null) {
            throw new XrapException("Timeout");
        }
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
            log.debug("socket is: " + sock);
            ByteBuffer bb = rep.getRouteid();
            if (bb != null) {
                sock.send(bb.array(), ZMQ.SNDMORE);
                sock.send(new byte[0], ZMQ.SNDMORE);
            } else {
                sock.send(new byte[0], ZMQ.SNDMORE);
            }
            sock.send(baos.toByteArray(), 0);
            log.debug("sendReply, size: " + baos.size());
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
                log.debug("sockDealer is: " + sockDealer);
                sockDealer.send(new byte[0], ZMQ.SNDMORE);
                sockDealer.send(baos.toByteArray(), 0);
                log.debug("sendOnly, size: " + baos.size());
            } finally {
                lock.unlock();
            }
        } else if (isServer) {
            try {
                lock.lock();
                log.debug("sockRouter is: " + sockRouter);
                sockRouter.send(request.getRouteid().array(), ZMQ.SNDMORE);
                sockRouter.send(new byte[0], ZMQ.SNDMORE);
                sockRouter.send(baos.toByteArray(), 0);
            } finally {
                lock.unlock();

            }
        }
    }

    public void sendAll(XrapRequest request) throws XrapException {
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
                log.debug("sockDealer is: " + sockDealer);
                sockDealer.send(new byte[0], ZMQ.SNDMORE);
                sockDealer.send(baos.toByteArray(), 0);
                log.debug("sendAll, size: " + baos.size());
            } finally {
                lock.unlock();
            }
        } else if (isServer) {
            try {
                lock.lock();
                log.debug("sockPub is: " + sockPub);
                sockPub.send("all".getBytes(), ZMQ.SNDMORE);
                sockPub.send(baos.toByteArray(), 0);
            } finally {
                lock.unlock();

            }
        }
    }

    public XrapReply sendAny(XrapRequest request) throws XrapException {
        if (!isServer) {
            throw new XrapException("Client cannot send to any!");
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            request.buildRequest(dos);
        } catch (IOException ex) {
            // shouldn't occur when writing to a ByteArrayOutputStream?
            throw new XrapException("Exception when building request");
        }
        try {
            lock.lock();
            log.debug("sockPush is: " + sockPub);
            sockPush.send(baos.toByteArray(), 0);
        } finally {
            lock.unlock();
        }

        int count = 0;
        int maxTries = 3;
        while(true) {
            try {
                XrapReply response = getResponse(request, receiveTimeout, receiveTimeoutUnit);
                if (response == null) {
                    throw new XrapException("Timeout");
                }
                return response;
            } catch (InterruptedException ex) {
                if(++count == maxTries){
                    log.error("getResponse was interrupted " + maxTries + " times, returning null.");
                    return null;
                }

            }
        }
    }



    private void sendOnly(ByteBuffer address, XrapRequest request) throws XrapException {
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
                log.debug("sockDealer is: " + sockDealer);
                sockDealer.send(new byte[0], ZMQ.SNDMORE);
                sockDealer.send(baos.toByteArray(), 0);
                log.debug("sendOnly, size: " + baos.size());
            } finally {
                lock.unlock();
            }
        } else if (isServer) {
            try {
                lock.lock();
                sockRouter.send(address.array(), ZMQ.SNDMORE);
                log.debug("sockRouter is: " + sockRouter);
                sockRouter.send(new byte[0], ZMQ.SNDMORE);
                sockRouter.send(baos.toByteArray(), 0);
            } finally {
                lock.unlock();

            }
        }
    }

    private XrapReply getResponse(XrapRequest request, long timeout, TimeUnit unit)
            throws XrapException, InterruptedException {
        XrapReply reply;
        log.debug("getResponse called, timeout: " + timeout + " " + unit);
        // There are two timeouts. We have to ensure that we return in a time
        // consistent with the timeout passed as argument. We first have to acquire the
        // lock. Another thread may have the lock, and may be waiting on a longer
        // timeout, waiting on the socket.

        long timeoutms = unit.toMillis(timeout);
        while (timeoutms > 0) {

            // First see whether the response has already been received, either
            // by us previously, or by another thread that might also be waiting.

            reply = responseCache.remove(request.getRequestId());
            if (reply != null) {
                log.debug("getResponse: response was in cache!\n");
                return reply;
            } else {
                log.debug("getResponse: not in cache\n");
            }
            synchronized (responseCache) {
                long loopStart = new java.util.Date().getTime();
                responseCache.wait(timeoutms);
                long loopStop = new java.util.Date().getTime();
                timeoutms -= (loopStop - loopStart);
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
        int threadnum = 1;
        Thread.currentThread().setName(String.format("XrapPeer-%d", threadnum));
        ctx = new ZContext();
        ZMQ.Poller items = new ZMQ.Poller(4);
        if (isServer) {
            log.info(
                    "Creating XRAP peer as server, binding to: " + String.format("tcp://%s:%d", host, port));
            sockRouter = ctx.createSocket(ZMQ.ROUTER);
            sockRouter.bind(String.format("tcp://%s:%d", host, port));
            sockPush = ctx.createSocket(ZMQ.PUSH);
            sockPush.bind(String.format("tcp://%s:%d", host, port + 1));
            sockPub = ctx.createSocket(ZMQ.PUB);
            sockPub.bind(String.format("tcp://%s:%d", host, port + 2));

            log.debug("Created sockRouter: " + sockRouter);
            log.debug("Created sockPush: " + sockPush);
            log.debug("Created sockPub: " + sockPub);
        } else {
            log.info(
                    "Creating XRAP peer as client, connecting to: "
                            + String.format("tcp://%s:%d", host, port));
            // If we're client (i.e. Net2Plan), connect to
            sockDealer = ctx.createSocket(ZMQ.DEALER);
            sockDealer.connect(String.format("tcp://%s:%d", host, port));
            sockPull = ctx.createSocket(ZMQ.PULL);
            sockPull.connect(String.format("tcp://%s:%d", host, port + 1));
            sockSub = ctx.createSocket(ZMQ.SUB);
            sockSub.connect(String.format("tcp://%s:%d", host, port + 2));
            sockSub.subscribe("all".getBytes());
            log.debug("Created sockDealer: " + sockDealer);
            log.debug("Created sockPull: " + sockPull);
            log.debug("Created sockSub: " + sockSub);
        }
        signal = ctx.createSocket(ZMQ.REP);
        signal.bind("inproc://signal");
        log.info("Connected signal: " + signal);
        // synchronize startup with creating thread
        setReady();
        log.warn("starting message processing");
        // Wait for new messages, receive them, and process
        while (!Thread.currentThread().isInterrupted() && !terminate) {

            int signalId;
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
                        log.debug("Router socket got message\n");
                        ByteBuffer routeId = ByteBuffer.wrap(sockRouter.recv());
                        byte[] empty = sockRouter.recv();
                        byte[] responseBytes = sockRouter.recv();
                        if (sockRouter.hasReceiveMore()) {
                            log.warn("sockRouter hasRecieveMore\n");
                        }
                        log.debug("Recieved data, len: " + responseBytes.length);
                        if (responseBytes.length > 0) {
                            XrapMessage msg = XrapMessage.parseRequest(ByteBuffer.wrap(responseBytes));
                            msg.setRouteid(routeId);
                            if (msg instanceof XrapReply) {
                                log.debug("Server got a XrapReply! Putting in the reply buffer..");
                                responseCache.put(msg.getRequestId(), (XrapReply) msg);
                                // notify any thread waiting for new messages
                                synchronized (responseCache) {
                                    responseCache.notify();
                                }
                            } else {
                                log.info("Got a request: \n" + msg.toString());
                                XrapReply rep = handleRequest(msg);
                                sendReply(sockRouter, rep);
                            }
                        }
                    }
                    if (items.pollin(pubId)) {
                        log.warn("Pub socket got message");
                    }
                    if (items.pollin(pushId)) {
                        log.warn("Push socket got message");
                    }
                    if (items.pollin(signalId)) {
                        log.warn("Got signal, aborting!");
                        //Thread.currentThread().interrupt();
                        terminate = true;
                    }
                } else if (!isServer) {
                    if (items.pollin(dealerId)) {
                        log.debug("Dealer socket got message");
                        byte[] empty = sockDealer.recv();
                        log.debug("empty frame: ");
                        byte[] responseBytes = sockDealer.recv();
                        if (responseBytes == null) {
                            log.warn("Got null from sockdealer.recv()!");
                            continue;
                        }
                        if (sockDealer.hasReceiveMore()) {
                            log.warn("sockRouter hasRecieveMore\n");
                        }
                        log.debug("Recieved data, len: " + responseBytes.length);
                        if (responseBytes.length > 0) {
                            XrapMessage msg = XrapMessage.parseRequest(ByteBuffer.wrap(responseBytes));
                            if (msg instanceof XrapReply) {
                                log.debug("Client got a XrapReply! Putting in the reply buffer..");

                                responseCache.put(msg.getRequestId(), (XrapReply) msg);
                                // notify any thread waiting for new messages
                                synchronized (responseCache) {
                                    responseCache.notify();
                                }
                            } else {
                                log.info("Client got a request: \n" + msg.toString());
                                XrapReply rep = handleRequest(msg);
                                sendReply(sockDealer, rep);
                            }
                        }
                    }
                    if (items.pollin(subId)) {
                        log.debug("Sub socket got message");
                        byte[] topic = sockSub.recv();
                        if (!new String(topic).equals("all")) {
                            log.warn("Got subscription with strange topic: " + new String(topic));
                        }
                        byte[] responseBytes = sockSub.recv();
                        if (responseBytes == null) {
                            log.warn("Got null from sockSub.recv()!");
                            continue;
                        }
                        if (sockSub.hasReceiveMore()) {
                            log.warn("sockSub hasRecieveMore\n");
                        }
                        log.info("Recieved sub data, len: " + responseBytes.length);
                        if (responseBytes.length > 0) {
                            XrapMessage msg = XrapMessage.parseRequest(ByteBuffer.wrap(responseBytes));
                            if (msg instanceof XrapReply) {
                                log.debug("Client got a XrapReply! Putting in the reply buffer..");

                                responseCache.put(msg.getRequestId(), (XrapReply) msg);
                                // notify any thread waiting for new messages
                                synchronized (responseCache) {
                                    responseCache.notify();
                                }
                            } else {
                                log.info("Client got a request: \n" + msg.toString());
                                XrapReply rep = handleRequest(msg);
                                sendReply(sockDealer, rep);
                            }
                        }
                    }
                    if (items.pollin(pullId)) {
                        log.warn("Pull socket got message");
                        byte[] responseBytes = sockPull.recv();
                        if (responseBytes == null) {
                            log.warn("Got null from sockPull.recv()!");
                            continue;
                        }
                        if (sockPull.hasReceiveMore()) {
                            log.warn("sockPull hasRecieveMore\n");
                        }
                        log.info("Recieved PUSH/PULL data, len: " + responseBytes.length);
                        if (responseBytes.length > 0) {
                            XrapMessage msg = XrapMessage.parseRequest(ByteBuffer.wrap(responseBytes));
                            if (msg instanceof XrapReply) {
                                log.debug("Client got a XrapReply! Putting in the reply buffer..");

                                responseCache.put(msg.getRequestId(), (XrapReply) msg);
                                // notify any thread waiting for new messages
                                synchronized (responseCache) {
                                    responseCache.notify();
                                }
                            } else {
                                log.info("Client got a request: \n" + msg.toString());
                                XrapReply rep = handleRequest(msg);
                                sendReply(sockDealer, rep);
                            }
                        }
                    }
                    if (items.pollin(signalId)) {
                        log.warn("Got signal, aborting!");
                        //Thread.currentThread().interrupt();
                        terminate = true;
                    }
                }
            /* } catch (e e) {
                log.error("items.poll() caught exception: " + e);
                Thread.currentThread().interrupt(); */
            } catch (XrapException e) {
                e.printStackTrace();
            }
        }

        log.info("Destroying ZeroMQ context..");
        ctx.destroy();
    }


    private XrapReply handleRequest(XrapMessage msg) {
        XrapReply rep = new XrapErrorReply(msg.getRequestId(), Constants.BadRequest_400, "Malformed message");
        rep.setRouteid(msg.getRouteid());
        if(type == callType.JAXRS) {
            if (msg instanceof XrapReply) {
                log.info("handleRequest JAXRS: got REPLY message: \n" + msg.toString() + "\n");
            } else if (msg instanceof XrapRequest) {
                log.info("handleRequest JAXRS: calling callJAXRS");
                XrapReply reppy = callJAXRS((XrapRequest) msg);
                if (reppy != null) {
                    rep = reppy;
                }
            } else {
                log.error("unknown XrapMessage type!");
            }
        } else {
            log.info("handleRequest XrapResource");
            XrapResourceService handle = findHandler(msg);
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


    private XrapResourceService trieLookup(String location) {
        // first check for perfect match
        XrapResourceService res = routeTrie.getValueForExactKey(location);
        if (res != null) {
            return res;
        }
        // Check for closest match, return first
        for (XrapResourceService r : routeTrie.getValuesForClosestKeys(location)) {
            return r;
        }
        // finally, look for default
        log.error("No handler found for \"" + location + "\", returning default");
        res = routeTrie.getValueForExactKey("/");
        return res;
    }

    private XrapResourceService findHandler(XrapMessage msg) {
        XrapResourceService handler = null;
        String location;
        if (msg instanceof XrapRequest) {
            location = ((XrapRequest) msg).getResource();
            log.debug("Looking for handler for route: " + location);
            handler = trieLookup(location);
        }
        if (handler != null) {
            log.debug("Found handler: " + handler.getClass());
        } else {
            log.error("Couldn't find any handler!");
        }

        return handler;
    }


    public void registerJAXRS(String packageToAdd, Object parent) {
        type = callType.JAXRS;
        log.debug("Looking for classes annotated with @Path");

        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(packageToAdd))
                .filterInputsBy(new FilterBuilder().includePackage(packageToAdd))
                .setScanners(
                        new SubTypesScanner(false),
                        new TypeAnnotationsScanner(),
                        new FieldAnnotationsScanner(),
                        new MethodAnnotationsScanner(),
                        new MethodParameterScanner(),
                        new MethodParameterNamesScanner(),
                        new MemberUsageScanner()));

        Set<Class<?>> annotated = reflections.getTypesAnnotatedWith(javax.ws.rs.Path.class);
        log.info("Found " + annotated.size() + " types annotated with @Path");
        for (Class c : annotated) {
            Annotation annotation = c.getDeclaredAnnotation(Path.class);
            javax.ws.rs.Path p = (Path) annotation;
            String rootUri = p.value();
            log.info(rootUri + " is handled by " + c.getName());
            for (Method met : c.getMethods()) {
                Annotation[] an = met.getDeclaredAnnotations();
                if (an.length > 0) {
                    Path pathA = met.getDeclaredAnnotation(Path.class);
                    String subPath = "";
                    String type = "unknown";

                    if (pathA != null)
                        subPath = pathA.value();

                    String regex = regexFromPath(subPath);
                    log.info("associating regex " + regex + " with " + met.getName());
                    Pattern pattern = Pattern.compile(regex);
                    urlPatterns.put(met, pattern);
                    if (met.getAnnotation(DELETE.class) != null) {
                        type = "DELETE";
                    } else if (met.getAnnotation(PUT.class) != null) {
                        type = "PUT";
                    } else if (met.getAnnotation(POST.class) != null) {
                        type = "POST";
                    } else if (met.getAnnotation(GET.class) != null) {
                        type = "GET";
                    }
                    log.info("    " + type + " " + rootUri + subPath + " -> " + met.getName());
                    Annotation[][] pan = met.getParameterAnnotations();
                    for (Annotation[] apa : pan) {
                        log.info("param annot: " + Arrays.toString(apa));
                    }

                    //updateOperationLog("    " + met.getName() + " annotated with " + Arrays.toString(an));
                }
            }
            try {
                // TODO: this looks scary..
                Class myClass = Class.forName(c.getName(), true, getClass().getClassLoader());
                Constructor constructor = myClass.getConstructor();
                Object instance = constructor.newInstance();
                log.info("instatiated class " + instance);

                if (instance instanceof IXrap) {
                    IXrap xac = (IXrap) instance;
                    log.info("Setting parent..");
                    xac.setParent(parent);
                    registeredClasses.put(rootUri, xac);
                } else {
                    log.warn("Not a IXrap instance!");
                    instance = null;
                }
            } catch (ClassNotFoundException e) {
                log.error("Could not instantiate " + c.getName());
                log.error(e.toString());
            } catch (NoSuchMethodException e) {
                log.error("Could not instantiate " + c.getName());
                log.error(e.toString());
            } catch (IllegalAccessException e) {
                log.error("Could not instantiate " + c.getName());
                log.error(e.toString());
            } catch (InstantiationException e) {
                log.error("Could not instantiate " + c.getName());
                log.error(e.toString());
            } catch (InvocationTargetException e) {
                log.error("Could not instantiate " + c.getName());
                log.error(e.toString());
                log.error(e.getCause().getMessage());
            }
        }
        printRegistered();
    }

    void printRegistered() {
        Iterable<KeyValuePair<IXrap>> clss = registeredClasses.getKeyValuePairsForKeysStartingWith("");
        for (KeyValuePair<IXrap> registered : clss) {
            log.debug("Registered URI: " + registered.getKey() + " class: " + registered.getValue().getClass().getName());
        }
    }


    private IXrap trieLookupRS(String location) {
        // first check for perfect match
        IXrap res = registeredClasses.getValueForExactKey(location);
        if (res != null) {
            return res;
        }
        // Check for closest match, return first
        for (IXrap r : registeredClasses.getValuesForClosestKeys(location)) {
            return r;
        }
        // finally, look for default
        return null;
    }

    private Method[] getTypeMethods(IXrap found, Class type) {
        ArrayList<Method> matching = new ArrayList<>();
        Method[] methods = found.getClass().getMethods();

        for (Method met : methods) {
            if (met.getAnnotation(type) != null) {
                matching.add(met);
            }
        }
        Method[] methods1 = new Method[matching.size()];
        methods1 = matching.toArray(methods1);
        return methods1;
    }


    // path = "/{linkId}/" -> ""/(?<linkId>[a-zA-Z][a-zA-Z0-9_-]*)/"
    private String regexFromPath(String path) {
        path = path.replace("{", "(?<");
        path = path.replace("}", ">[a-zA-Z0-9_-][a-zA-Z0-9_-]*)");
        return path;
    }

    private XrapReply callJAXRS(XrapRequest message) { //Class type, String uri, String body){
        Class type = null;
        String uri = null;
        String body = "";
        if (message instanceof XrapGetRequest) {
            uri = message.getResource();
            type = GET.class;
        } else if (message instanceof XrapPutRequest) {
            uri = message.getResource();
            // TODO: assumes its string data, should check content type.
            body = new String(((XrapPutRequest) message).getContentBody());
            type = PUT.class;
        } else if (message instanceof XrapPostRequest) {
            uri = message.getResource();
            // TODO: assumes its string data, should check content type.
            body = new String(((XrapPostRequest) message).getContentBody());
            type = POST.class;
        } else if (message instanceof XrapDeleteRequest) {
            uri = message.getResource();
            type = DELETE.class;
        }
        if (type == null) {
            log.error("Unknow XrapRequest type!");
            return new XrapErrorReply(message.getRequestId(),(short) 404, "Unknown XrapRequest type");
        }

        log.debug("callJAXRS(" + type.getName() + ", " + uri + " ," + body + ")");

        IXrap apiClass = trieLookupRS(uri);
        if (apiClass == null) {
            log.error("Could not find handler for " + type + " " + uri);
            return new XrapErrorReply(message.getRequestId(),(short) 404, "Could not find handler for "+ uri);
        }
        String rootURL = null;
        Path pathAnnotation = apiClass.getClass().getAnnotation(Path.class);
        if (pathAnnotation == null) {
            log.error("Could not find root URI for " + apiClass.getClass().getName());
            return new XrapErrorReply(message.getRequestId(),(short) 404, "Could not find root URI for " + apiClass.getClass().getName());
        }
        rootURL = pathAnnotation.value();
        String strippedUri = uri.replace(rootURL, "");
        log.debug("Stripped URI: " + strippedUri);
        Method[] matchingMethods = getTypeMethods(apiClass, type);
        if (matchingMethods.length == 0) {
            log.error("No matching methods, generate an error response");
            return new XrapErrorReply(message.getRequestId(),(short) 404, "No matching methods in " + apiClass.toString());
        }
        for (Method candidateMethod : matchingMethods) {
            log.debug("Matching methods: " + candidateMethod.getName());

            // check that it actually returns a response
            if (candidateMethod.getReturnType() != Response.class) {
                log.error("registered method " + candidateMethod.getName() + " returns " + candidateMethod.getReturnType().getName());
                log.error("when it should return javax.ws.rs.Response!");
                continue;
            }

            Path pathA = candidateMethod.getDeclaredAnnotation(Path.class);
            String subPath = "";
            if (pathA != null)
                subPath = pathA.value();


            String completeURL = rootURL + subPath;
            log.debug("TotalURL :" + regexFromPath(completeURL));

            Pattern regex = urlPatterns.get(candidateMethod);
            if (regex == null) {
                log.error("Could not find regex pattern for " + candidateMethod.getName());
                continue;
            }
            Matcher matcher = regex.matcher(strippedUri);
            if (!matcher.matches()) {
                log.debug("No regex match for " + candidateMethod.getName() + ", trying next..");
                continue;
            }
            // We've found the method we should call, prepare the arguments
            int numParams = candidateMethod.getParameterCount();
            // no parameters, call directly
            Object[] invokeParams;
            ArrayList<Object> paramList = new ArrayList<>();
            if (numParams > 0) {
                Parameter[] methodParameters = candidateMethod.getParameters();
                int groupIndex = 1;
                for (Parameter param : methodParameters) {
                    // Path parameter, get from matcher
                    PathParam pp = param.getDeclaredAnnotation(PathParam.class);
                    if (pp != null) {
                        String pathParam = matcher.group(groupIndex);
                        groupIndex++;
                        // Should we cast it

                        Object castObject = castParameter(pathParam, param.getParameterizedType());
                        if (castObject == null) {
                            log.error("Could not cast path parameter! ");
                            return new XrapErrorReply(message.getRequestId(),(short) 404, "could not cast path parameter " + pathParam);

                        }
                        paramList.add(castObject);
                    }
                    // Query parameter, get from XrapMsg..
                    else if (param.getDeclaredAnnotation(QueryParam.class) != null) {
                        log.error("Query Parameter, not handled! " + param.getName());
                        return new XrapErrorReply(message.getRequestId(),(short) 404, "Query parameter. not handled " + param.toString());
                    } else {
                        Object castObject = castParameter(body, param.getParameterizedType());
                        if (castObject == null) {
                            log.error("Could not cast body parameter!");
                            return new XrapErrorReply(message.getRequestId(),(short) 404, "Could not cast body parameter to type " + param.getParameterizedType().toString());
                        }
                        paramList.add(castObject);
                    }
                }
            }

            invokeParams = new Object[paramList.size()];
            invokeParams = paramList.toArray(invokeParams);
            try {
                log.info("Invoking method " + candidateMethod.getName());
              /*  log.info("With parameters " + Arrays.toString(invokeParams));*/
                Response response = (Response) candidateMethod.invoke(apiClass, invokeParams);
                /*log.info("#########################");
                log.info(response.toString());
                if (response.hasEntity()) {
                    log.info("Body: " + response.getEntity());
                }
                log.info("#########################");
                */
                return responseToXrapReply(message, response);
            } catch (IllegalAccessException e) {
                log.error("Couid not invoke method! ");
                log.info("With parameters " + Arrays.toString(invokeParams));
                log.error(e.toString());
                return new XrapErrorReply(message.getRequestId(),(short) 404, "Could not invoke method " + e.toString());

            } catch (InvocationTargetException e) {
                log.error("Couid not invoke method! ");
                log.info("With parameters " + Arrays.toString(invokeParams));
                e.printStackTrace();
                log.error(e.toString());
                log.error(e.getCause().getMessage());
                return new XrapErrorReply(message.getRequestId(),(short) 404, "Could not invoke method " + e.getTargetException().getMessage());

            }
        }
        return new XrapErrorReply(message.getRequestId(),(short) 404, "Could not find mathcing methods!");

    }

    private Object castParameter(String paramData, Type type) {

        if (type == String.class) {
            return paramData;
        } else if (type == Long.class) {
            return Long.getLong(paramData);
        } else if (type == Double.class) {
            return Double.parseDouble(paramData);
        } else if (type == Integer.class) {
            return Integer.parseInt(paramData);
        } else {
            try {
                Object parsedJson = json.deserialize(paramData, type);
/*
                log.warn("Deserialized JSON: " + paramData);
                log.warn("Resulting object: " + parsedJson.getClass().getName());
                log.warn("Result: " + parsedJson.toString());
   */
                return parsedJson;
            } catch (JsonIOException e) {
                log.error("Couldn't convert parameter " + paramData + " to type " + type.getTypeName());
                log.error("Could not parse JSON ");
                log.error(e.toString());
                return null;
            } catch (JsonSyntaxException e) {
                log.error("Couldn't convert parameter " + paramData + " to type " + type.getTypeName());
                log.error("Could not parse JSON ");
                log.error(e.toString());
                return null;
            }
        }
    }

    /*private Object castParameterType(String paramData, Type type) {

        if (type == String.class) {
            return paramData;
        } else if (type == Long.class) {
            return Long.getLong(paramData);
        } else if (type == Double.class) {
            return Double.parseDouble(paramData);
        } else if (type == Integer.class) {
            return Integer.parseInt(paramData);
        } else {
            try {
                Object parsedJson = json.deserialize(paramData, type);
                log.warn("Deserialized JSON: " + paramData);
                log.warn("Resulting object: " + parsedJson.getClass().getName());
                log.warn("Result: " + parsedJson.toString());
                return parsedJson;
            } catch (JsonIOException e) {
                log.error("Couldn't convert parameter " + paramData + " to type " + type.getName());
                log.error("Could not parse JSON ");
                log.error(e.toString());
                return null;
            } catch (JsonSyntaxException e) {
                log.error("Couldn't convert parameter " + paramData + " to type " + type.getName());
                log.error("Could not parse JSON ");
                log.error(e.toString());
                return null;
            }
        }
    }*/

    // Convert a JAX-RS response to XrapReply
    private XrapReply responseToXrapReply(XrapMessage message, Response response) {
        if (response == null) {
            XrapErrorReply xer = new XrapErrorReply(message.getRequestId(), (short) 400, "Could not process request");
            xer.setRouteid(message.getRouteid());
            return xer;
        }

        // Treat anything over 200 as an error
        if (response.getStatus() > 299) {
            XrapErrorReply xer = new XrapErrorReply(message.getRequestId(), (short) response.getStatus(), response.getStatusInfo().getReasonPhrase());
            xer.setRouteid(message.getRouteid());
            return xer;
        }
        if (message instanceof XrapGetRequest) {
            XrapGetReply reply = new XrapGetReply();
            reply.setRouteid(message.getRouteid());
            reply.setRequestId(message.getRequestId());
            reply.setStatusCode((short) response.getStatus());
            if (response.hasEntity()) {
                // Should convert to JSON if possible
                Object ent = response.getEntity();
                if(ent instanceof String){
                    reply.setBody(((String) ent).getBytes());
                } else {
                    String bodystr = json.serialize(response.getEntity());
                    if (bodystr != null) {
                        reply.setBody(bodystr.getBytes());
                    } else {
                        log.error("Could not serialize body " + ent);
                    }
                }
            } else {
                reply.setBody("".getBytes());
            }
            if (response.getMediaType() != null) {
                reply.setContentType(response.getMediaType().toString());
            } else {
                // Default to json
                reply.setContentType("application/json");
            }

            if (response.getLastModified() != null) {
                reply.setDateModified(response.getLastModified().getTime());
            } else {
                // default to 'now'
                reply.setDateModified(new Date().getTime());
            }
            if (response.getEntityTag() != null) {
                reply.setEtag(response.getEntityTag().getValue());
            } else if (response.hasEntity()) {
                // put the hashcode if nothing else
                reply.setEtag(Integer.toString(response.getEntity().hashCode()));
            }
            List<NameValuePair> metadata = new ArrayList<>();
            for (String key : response.getMetadata().keySet()) {
                metadata.add(new NameValuePair(key, response.getMetadata().get(key).toString()));
            }
            NameValuePair metadata1[] = new NameValuePair[metadata.size()];
            metadata1 = metadata.toArray(metadata1);
            reply.setMetadata(metadata1);
            return reply;
        } else if (message instanceof XrapPostRequest) {
            XrapPostReply reply = new XrapPostReply();
            reply.setRouteid(message.getRouteid());
            reply.setRequestId(message.getRequestId());
            reply.setStatusCode((short) response.getStatus());
            if (response.hasEntity()) {
                // Should convert to JSON if possible
                Object ent = response.getEntity();
                if(ent instanceof String){
                    reply.setBody(((String) ent).getBytes());
                } else {
                    String bodystr = json.serialize(response.getEntity());
                    if (bodystr != null) {
                        reply.setBody(bodystr.getBytes());
                    } else {
                        log.error("Could not serialize body " + ent);
                    }
                }
            } else {
                reply.setBody("".getBytes());
            }
            if (response.getMediaType() != null)
                reply.setContentType(response.getMediaType().toString());

            if (response.getLastModified() != null) {
                reply.setDateModified(response.getLastModified().getTime());
            } else {
                // default to 'now'
                reply.setDateModified(new Date().getTime());
            }
            if (response.getEntityTag() != null) {
                reply.setEtag(response.getEntityTag().getValue());
            } else if (response.hasEntity()) {
                // put the hashcode if nothing else
                reply.setEtag(Integer.toString(response.getEntity().hashCode()));
            }
            List<NameValuePair> metadata = response.getMetadata().keySet().stream().map(
                    key -> new NameValuePair(key, response.getMetadata().get(key).toString())).collect(Collectors.toList());
            NameValuePair metadata1[] = new NameValuePair[metadata.size()];
            metadata1 = metadata.toArray(metadata1);
            reply.setMetadata(metadata1);
            return reply;
        } else if (message instanceof XrapPutRequest) {
            XrapPutReply reply = new XrapPutReply();
            reply.setRouteid(message.getRouteid());
            reply.setRequestId(message.getRequestId());
            reply.setStatusCode((short) response.getStatus());
            if (response.getLastModified() != null) {
                reply.setDateModified(response.getLastModified().getTime());
            } else {
                // default to 'now'
                reply.setDateModified(new Date().getTime());
            }
            if (response.getEntityTag() != null) {
                reply.setEtag(response.getEntityTag().getValue());
            } else if (response.hasEntity()) {
                // put the hashcode if nothing else
                reply.setEtag(Integer.toString(response.getEntity().hashCode()));
            }
            List<NameValuePair> metadata = response.getMetadata().keySet().stream().map(
                    key -> new NameValuePair(key, response.getMetadata().get(key).toString())).collect(Collectors.toList());
            NameValuePair metadata1[] = new NameValuePair[metadata.size()];
            metadata1 = metadata.toArray(metadata1);
            reply.setMetadata(metadata1);
            return reply;
        } else if (message instanceof XrapDeleteRequest) {
            XrapDeleteReply reply = new XrapDeleteReply();
            reply.setRouteid(message.getRouteid());
            reply.setRequestId(message.getRequestId());
            reply.setStatusCode((short) response.getStatus());
            List<NameValuePair> metadata = response.getMetadata().keySet().stream().map(
                    key -> new NameValuePair(key, response.getMetadata().get(key).toString())).collect(Collectors.toList());
            NameValuePair metadata1[] = new NameValuePair[metadata.size()];
            metadata1 = metadata.toArray(metadata1);
            reply.setMetadata(metadata1);
            return reply;
        }

        XrapErrorReply xer = new XrapErrorReply(message.getRequestId(), (short) 400, "Could not process request");
        xer.setRouteid(message.getRouteid());
        return xer;
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
                if (response == null) {
                    response = getResponse(request, 0, TimeUnit.SECONDS);
                }
            } catch (XrapException ex) {
                this.ex = ex;
            } catch (InterruptedException ex) {
            }
            return response != null;
        }

        @Override
        public XrapReply get() throws InterruptedException, ExecutionException {
            try {
                if (ex != null) {
                    throw ex;
                }
                if (response == null) {
                    response = getResponse(request);
                }
                return response;
            } catch (XrapException ex) {
                throw new ExecutionException(ex);
            }
        }

        @Override
        public XrapReply get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            try {
                if (ex != null) {
                    throw ex;
                }
                if (response == null) {
                    response = getResponse(request, timeout, unit);
                }
                if (response == null) {
                    throw new TimeoutException();
                }
                return response;
            } catch (XrapException ex) {
                throw new ExecutionException(ex);
            }
        }

    }
}
