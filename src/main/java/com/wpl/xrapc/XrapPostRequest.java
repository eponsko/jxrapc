package com.wpl.xrapc;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Represents a POST request to be sent to an XRAP server.
 *
 * @author tomq
 */
public class XrapPostRequest extends XrapRequest {
    private String contentType;
    private byte[] contentBody;

    /**
     * Constructs a new POST request.
     *
     * @param parentResource The parent resource that the POST will be send to GET.
     *                       This is a string of the form /a/b/c
     */
    public XrapPostRequest(String parentResource) {
        super(parentResource);
    }

    public XrapPostRequest() {
    }

    public XrapPostRequest(String resource, String body) {
        super(resource);
        contentType = "application/json";
        contentBody = body.getBytes();
    }

    public String toString() {
        String top = super.toString();
        StringBuilder sb = new StringBuilder();
        sb.append("Command: POST\n");
        sb.append("Content-type: " + contentType + "\n");
        if (contentType.equals("application/json")) {
            sb.append("Content-body: " + new String(contentBody) + "\n");
        } else {
            sb.append("Content-body: " + contentBody + "\n");
        }
        sb.append("Type: POST\n");
        return top + sb.toString();
    }

    @Override
    void buildRequest(DataOutputStream dos) throws IOException {
        dos.writeShort(Constants.SIGNATURE);
        dos.writeByte(Constants.POST_COMMAND);
        dos.writeInt(getRequestId());
        writeString(dos, getResource());
        writeString(dos, getContentType());
        writeLongString(dos, getContentBody());
    }

    /**
     * Returns the content type specified in this request.
     *
     * @return
     */
    public String getContentType() {
        return contentType;
    }

    /**
     * Sets the content type of the body being sent.
     *
     * @param type The content type.
     */
    public void setContentType(String type) {
        this.contentType = type;
    }

    /**
     * Returns the content body specified in this request.
     *
     * @return
     */
    public byte[] getContentBody() {
        return contentBody;
    }

    /**
     * Sets the content body to be posted in the request.
     *
     * @param body The body, as binary bytes.
     */
    public void setContentBody(byte[] body) {
        this.contentBody = body;
    }

    @Override
    XrapReply parseResponse(ByteBuffer buffer) throws XrapException {
        checkSignature(buffer);
        int command = buffer.get();
        if (command == Constants.ERROR_COMMAND) {
            return readErrorResponse(buffer);
        } else if (command == Constants.POST_OK_COMMAND) {
            XrapPostReply response = new XrapPostReply();
            response.setRequestId(buffer.getInt());
            response.setStatusCode(buffer.getShort());
            response.setLocation(readString(buffer));
            response.setEtag(readString(buffer));
            response.setDateModified(buffer.getLong());
            response.setContentType(readString(buffer));
            response.setBody(readLongBinaryString(buffer));
            response.setMetadata(readHash(buffer));
            return response;
        } else {
            throw new UnknownResponseCodeException("POST", command);
        }
    }

}
