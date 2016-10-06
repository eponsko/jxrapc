package com.wpl.xrapc;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

public class XrapPostReply extends XrapReply {
    private NameValuePair[] metadata;
    private String etag;
    private String location;
    private long dateModified;
    private String contentType;
    private byte[] body;

    public XrapPostReply(int requestId, short statusCode, String resource, String etag, long dateModified, String contentType, byte[] contentBody, NameValuePair[] metadata) {
        setRequestId(requestId);
        setLocation(resource);
        setStatusCode(statusCode);
        setEtag(etag);
        setDateModified(dateModified);
        setContentType(contentType);
        setBody(contentBody);
        setMetadata(metadata);
    }

    public XrapPostReply() {
    }

    public NameValuePair[] getMetadata() {
        return metadata;
    }

    public void setMetadata(NameValuePair[] metadata) {
        this.metadata = metadata;
    }

    public String getEtag() {
        return etag;
    }

    public void setEtag(String etag) {
        this.etag = etag;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
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

    ;

    void buildReply(DataOutputStream dos) throws IOException {
        dos.writeShort(Constants.SIGNATURE);
        dos.writeByte(Constants.POST_OK_COMMAND);
        dos.writeInt(getRequestId());
        dos.writeShort(getStatusCode());
        writeString(dos, getLocation());
        writeString(dos, getEtag());
        dos.writeLong(getDateModified());
        writeString(dos, getContentType());
        writeLongString(dos, getBody());
        writeHash(dos, getMetadata());
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Type: POST-OK\n");

        if (getLocation() != null) {
            sb.append("Location: " + getLocation().toString() + "\n");
        }
        if (getEtag() != null) {
            sb.append("Etag: " + getEtag() + "\n");
        }

        sb.append("Date modified: " + new Date(getDateModified()).toString() + "\n");
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

