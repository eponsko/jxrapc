package com.wpl.xrapc;

import org.apache.commons.codec.binary.Hex;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Date;

/**
 * Created by eponsko on 2016-09-01.
 */
public class XrapMessage {
    private static Charset utf8 = Charset.forName("UTF8");
    private ByteBuffer routeid = null;
    private int requestId;

    public static XrapMessage parseRequest(ByteBuffer buffer) throws XrapException {
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
        } else if (command == Constants.ERROR_COMMAND) {
            XrapErrorReply response = new XrapErrorReply();
            response.setRequestId(buffer.getInt());
            response.setStatusCode(buffer.getShort());
            response.setErrorText(sreadString(buffer));
            return response;
        } else if (command == Constants.POST_OK_COMMAND) {
            XrapPostReply response = new XrapPostReply();
            response.setRequestId(buffer.getInt());
            response.setStatusCode(buffer.getShort());
            response.setLocation(sreadString(buffer));
            response.setEtag(sreadString(buffer));
            response.setDateModified(buffer.getLong());
            response.setContentType(sreadString(buffer));
            response.setBody(sreadLongBinaryString(buffer));
            response.setMetadata(readHash(buffer));
            return response;
        } else if (command == Constants.GET_OK_COMMAND) {
            XrapGetReply response = new XrapGetReply();
            response.setRequestId(buffer.getInt());
            response.setStatusCode(buffer.getShort());
            response.setEtag(sreadString(buffer));
            response.setDateModified(buffer.getLong());
            response.setContentType(sreadString(buffer));
            response.setBody(sreadLongBinaryString(buffer));
            response.setMetadata(readHash(buffer));
            return response;
        } else if (command == Constants.PUT_OK_COMMAND) {
            XrapPutReply response = new XrapPutReply();
            response.setRequestId(buffer.getInt());
            response.setStatusCode(buffer.getShort());
            response.setLocation(sreadString(buffer));
            response.setEtag(sreadString(buffer));
            response.setDateModified(buffer.getLong());
            response.setMetadata(readHash(buffer));
            return response;
        } else if (command == Constants.GET_EMPTY_COMMAND) {
            XrapGetEmptyReply response = new XrapGetEmptyReply();
            response.setRequestId(buffer.getInt());
            response.setStatusCode(buffer.getShort());
            return response;
        } else if (command == Constants.DELETE_OK_COMMAND) {
            XrapDeleteReply response = new XrapDeleteReply();
            response.setRequestId(buffer.getInt());
            response.setStatusCode(buffer.getShort());
            response.setMetadata(readHash(buffer));
            return response;
        } else {
            throw new UnknownResponseCodeException("Unknown", command);
        }
    }

    protected static void checkSignature(ByteBuffer dis) throws XrapException {
        short signature = dis.getShort();
        if (signature != Constants.SIGNATURE) {
            throw new InvalidSignatureException(signature);
        }
    }

    protected static String sreadString(ByteBuffer dis) {
        int length = dis.get() & 0xff;
        byte[] stringBytes = new byte[length];
        dis.get(stringBytes);
        return new String(stringBytes, utf8);
    }

    protected static byte[] sreadLongBinaryString(ByteBuffer dis) {
        int length = dis.getInt();
        if (length > dis.remaining()) {
            // TODO:
        }
        byte[] stringBytes = new byte[length];
        dis.get(stringBytes);
        return stringBytes;
    }

    protected static NameValuePair[] readHash(ByteBuffer buffer) {
        int count = buffer.getInt();
        // for each entry there is a short string and a long string
        // So the minimum number of bytes we require is count*5
        if (buffer.remaining() < count * 5) {
            // TODO
        }

        NameValuePair[] result = new NameValuePair[count];
        for (int i = 0; i < count; i++) {
            String name = sreadString(buffer);
            result[i] = new NameValuePair(name, sreadLongBinaryString(buffer));
        }
        return result;
    }

    protected static void writeHash(DataOutputStream dos, NameValuePair[] metadata)
            throws IOException {
        if (metadata == null) {
            dos.writeInt(0);
        } else {
            dos.writeInt(metadata.length);
            for (NameValuePair nvp : metadata) {
                writeString(dos, nvp.getName());
                writeLongString(dos, nvp.getRawValue());
            }
        }
    }

    protected static void writeString(DataOutputStream dos, String s) throws IOException {
        writeString(dos, s == null ? null : s.getBytes(utf8));
    }

    protected static void writeString(DataOutputStream dos, byte[] bytes) throws IOException {
        if (bytes == null) {
            dos.writeByte(0);
            return;
        }
        if (bytes.length > 255) {
            throw new IllegalArgumentException();
        }
        dos.writeByte(bytes.length);
        dos.write(bytes);
    }

    protected static void writeLongString(DataOutputStream dos, String s) throws IOException {
        writeLongString(dos, s == null ? null : s.getBytes(utf8));
    }

    protected static void writeLongString(DataOutputStream dos, byte[] bytes) throws IOException {
        if (bytes == null) {
            dos.writeInt(0);
            return;
        }
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    public int getRequestId() {
        return requestId;
    }

    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Request-Id: " + getRequestId() + "\n");
        if (routeid != null) {
            sb.append("RouterId: " + Hex.encodeHexString(getRouteid().array()) + "\n");
        }

        return sb.toString();
    }

    public ByteBuffer getRouteid() {
        return routeid;
    }

    public void setRouteid(ByteBuffer routeid) {
        this.routeid = routeid;
    }

    protected XrapReply sreadErrorResponse(ByteBuffer dis) {
        XrapErrorReply response = new XrapErrorReply();
        response.setRequestId(dis.getInt());
        response.setStatusCode(dis.getShort());
        response.setErrorText(sreadString(dis));
        return response;
    }

    protected void writeParameter(DataOutputStream dos, XrapGetRequest.Parameter p)
            throws IOException {
        writeString(dos, p.getName());
        if (p.isBinary()) {
            writeLongString(dos, p.getBinaryValue());
        } else {
            writeLongString(dos, p.getStringValue());
        }
    }

    protected String readString(ByteBuffer dis) {
        int length = dis.get() & 0xff;
        byte[] stringBytes = new byte[length];
        dis.get(stringBytes);
        return new String(stringBytes, utf8);
    }

    protected byte[] readLongBinaryString(ByteBuffer dis) {
        int length = dis.getInt();
        if (length > dis.remaining()) {
            // TODO:
        }
        byte[] stringBytes = new byte[length];
        dis.get(stringBytes);
        return stringBytes;
    }

}
