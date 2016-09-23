package com.wpl.xrapc;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for the classes that represent the
 * different types of XRAP request.
 *
 * @author tomq
 */
public abstract class XrapRequest extends XrapMessage {
    private static Charset utf8 = Charset.forName("UTF8");
    private String resource;
    private int requestId;
    private static AtomicInteger nextRequestId = new AtomicInteger(1);

    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (resource != null)
            sb.append("Resource: " + resource + "\n");
        sb.append("RequestId: " + requestId + "\n");

        return sb.toString();
    }

    protected XrapRequest(String resource) {
        this.resource = resource;
        this.requestId = nextRequestId.getAndIncrement();
    }

    protected XrapRequest() {
    }

    /**
     * Returns the request ID for this request.
     *
     * @return The request ID.
     */
    public int getRequestId() {
        return requestId;
    }

    public void setRequestId(int id) {
        requestId = id;
    }

    /**
     * Sets the resource that is the subject of the request.
     * This is a string of the form /a/b/c.
     * The resource string is always passed to the server as a UTF8 string.
     *
     * @param resource The new resource.
     */
    public void setResource(String resource) {
        this.resource = resource;
    }

    /**
     * Returns the resource that is the subject of the request.
     *
     * @return The resource
     */
    public String getResource() {
        return resource;
    }

    abstract void buildRequest(DataOutputStream dos) throws IOException;

    abstract XrapReply parseResponse(ByteBuffer response) throws XrapException;

    public static XrapRequest parseRequest(ByteBuffer routeid, ByteBuffer buffer) throws XrapException {
        XrapRequest rep = parseRequest(buffer);
        rep.setRouteid(routeid);
        return rep;
    }


    public static XrapRequest parseRequest(ByteBuffer buffer) throws XrapException {
        buffer.order(ByteOrder.BIG_ENDIAN);
        short signature = buffer.getShort();
        if (signature != Constants.SIGNATURE) {
            throw new InvalidSignatureException(signature);
        }
        int command = buffer.get();
        // Check if POST_COMMAND, if so, parse it
        if (command == Constants.POST_COMMAND) {
            XrapPostRequest postreq = new XrapPostRequest();
            postreq.setRequestId(buffer.getInt());
            postreq.setResource(sreadString(buffer));
            postreq.setContentType(sreadString(buffer));
            postreq.setContentBody(sreadLongBinaryString(buffer));
            return postreq;
        } else if (command == Constants.GET_COMMAND) {
            XrapGetRequest getreq = new XrapGetRequest();
            getreq.setRequestId(buffer.getInt());
            getreq.setResource(sreadString(buffer));
            int numParams = buffer.getInt();
            while (numParams > 0) {
                String name = sreadString(buffer);
                byte[] value = sreadLongBinaryString(buffer);
                getreq.addParameter(name, new String(value));
                numParams--;
            }
            getreq.setIfModifiedSince(new Date(buffer.getLong()));
            getreq.setIfNoneMatch(sreadString(buffer));
            getreq.setContentType(sreadString(buffer));
            return getreq;
        } else if (command == Constants.DELETE_COMMAND) {
            XrapDeleteRequest delreq = new XrapDeleteRequest();
            delreq.setRequestId(buffer.getInt());
            delreq.setResource(sreadString(buffer));
            delreq.setIfUnmodifiedSince(new Date(buffer.getLong()));
            delreq.setIfMatch(sreadString(buffer));
            return delreq;

        } else if (command == Constants.PUT_COMMAND) {
            XrapPutRequest putreq = new XrapPutRequest();
            putreq.setRequestId(buffer.getInt());
            putreq.setResource(sreadString(buffer));
            putreq.setIfUnmodifiedSince(new Date(buffer.getLong()));
            putreq.setIfMatch(sreadString(buffer));
            putreq.setContentType(sreadString(buffer));
            putreq.setContentBody(sreadLongBinaryString(buffer));
            return putreq;
        } else {
            throw new UnknownResponseCodeException("POST", command);
        }
    }

    XrapReply parseResponse(byte[] responseBytes) throws XrapException {
        ByteBuffer response = ByteBuffer.wrap(responseBytes);
        response.order(ByteOrder.BIG_ENDIAN);
        return parseResponse(response);
    }

    XrapRequest parseRequest(byte[] requestBytes) throws XrapException {
        ByteBuffer request = ByteBuffer.wrap(requestBytes);
        request.order(ByteOrder.BIG_ENDIAN);
        return parseRequest(request);
    }



    protected XrapReply readErrorResponse(ByteBuffer dis) {
        XrapErrorReply response = new XrapErrorReply();
        response.setRequestId(dis.getInt());
        response.setStatusCode( dis.getShort());
        response.setErrorText(readString(dis));
        return response;
    }
}
