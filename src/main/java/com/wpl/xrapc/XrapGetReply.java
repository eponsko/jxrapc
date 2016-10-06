package com.wpl.xrapc;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

public class XrapGetReply extends XrapReply {
    private String etag;
    private long dateModified;
    private String contentType;
    private byte[] body;
    private NameValuePair[] metadata;

    public XrapGetReply(int requestId, short statusCode, String etag, long dateModified, String contentType, byte[] contentBody, NameValuePair[] metadata) {
        setRequestId(requestId);
        setStatusCode(statusCode);
        setEtag(etag);
        setDateModified(dateModified);
        setContentType(contentType);
        setBody(contentBody);
        setMetadata(metadata);
    }

    public XrapGetReply() {
    }

    void buildReply(DataOutputStream dos) throws IOException {
        dos.writeShort(Constants.SIGNATURE);
        dos.writeByte(Constants.GET_OK_COMMAND);
        dos.writeInt(getRequestId());
        dos.writeShort(getStatusCode());
        writeString(dos, getEtag());
        dos.writeLong(getDateModified());
        writeString(dos, getContentType());
        writeLongString(dos, getBody());
        writeHash(dos, getMetadata());
    }

    public String getEtag() {
        return etag;
    }

    public void setEtag(String etag) {
        this.etag = etag;
    }

    public long getDateModified() {
        return dateModified;
    }

    public void setDateModified(long dateModified) {
        this.dateModified = dateModified;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public NameValuePair[] getMetadata() {
        return metadata;
    }


    public void setMetadata(NameValuePair[] metadata) {
        this.metadata = metadata;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("Type: GET-OK\n");
        if (getEtag() != null) {
            sb.append("Etag: " + getEtag() + "\n");
        }

        sb.append("Date modified: " + new Date(getDateModified()).toString() + "\n");

        if (getContentType() != null) {
            sb.append("Content-Type: " + getContentType() + "\n");
        }
        if (getBody() != null) {
            sb.append("Body: " + new String(getBody()) + "\n");
        }

        if (getMetadata() != null) {
            for (NameValuePair nvp : getMetadata()) {
                sb.append("Meta: " + nvp.getName() + " Value: " + nvp.getStringValue() + "\n");
            }
        }

        return super.toString() + sb.toString();
    }
}

