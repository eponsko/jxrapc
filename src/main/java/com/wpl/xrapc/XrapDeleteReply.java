package com.wpl.xrapc;

import java.io.DataOutputStream;
import java.io.IOException;

public class XrapDeleteReply extends XrapReply {

    public NameValuePair[] metadata;

    public XrapDeleteReply(int requestId, short statusCode, NameValuePair[] metadata) {
        setRequestId(requestId);
        setStatusCode(statusCode);
        setMetadata(metadata);
    }

    public XrapDeleteReply() {
    }

    void buildReply(DataOutputStream dos) throws IOException {
        dos.writeShort(Constants.SIGNATURE);
        dos.writeByte(Constants.DELETE_OK_COMMAND);
        dos.writeInt(getRequestId());
        dos.writeShort(getStatusCode());
        writeHash(dos, getMetadata());
    }

    public NameValuePair[] getMetadata() {
        return metadata;
    }

    public void setMetadata(NameValuePair[] metadata) {
        this.metadata = metadata;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("Type: DELETE-OK\n");

        if (getMetadata() != null) {
            for (NameValuePair nvp : getMetadata()) {
                sb.append("Meta: " + nvp.getName() + " Value: " + nvp.getStringValue() + "\n");
            }
        }

        return super.toString() + sb.toString();
    }
}
