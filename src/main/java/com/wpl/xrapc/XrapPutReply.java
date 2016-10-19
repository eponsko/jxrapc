package com.wpl.xrapc;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

public class XrapPutReply extends XrapReply {
    private String etag;
    private String location;
    private long dateModified;
    private NameValuePair[] metadata;

    public XrapPutReply(
            int requestId,
            short statusCode,
            String location,
            String etag,
            long dateModified,
            NameValuePair[] metadata) {
        setRequestId(requestId);
        setStatusCode(statusCode);
        setLocation(location);
        setEtag(etag);
        setDateModified(dateModified);
        setMetadata(metadata);
    }

    public XrapPutReply() {
    }

    public void buildReply(DataOutputStream dos) throws IOException {
        dos.writeShort(Constants.SIGNATURE);
        dos.writeByte(Constants.PUT_OK_COMMAND);
        dos.writeInt(getRequestId());
        dos.writeShort(getStatusCode());
        XrapMessage.writeString(dos, getLocation());
        XrapMessage.writeString(dos, getEtag());
        dos.writeLong(getDateModified());
        XrapMessage.writeHash(dos, getMetadata());
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
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

    public NameValuePair[] getMetadata() {
        return metadata;
    }

    public void setMetadata(NameValuePair[] metadata) {
        this.metadata = metadata;
    }


    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Type: PUT-OK\n");
        if (getLocation() != null) {
            sb.append("Location: " + getLocation().toString() + "\n");
        }
        if (getEtag() != null) {
            sb.append("Etag: " + getEtag() + "\n");
        }

        sb.append("Date modified: " + new Date(getDateModified()).toString() + "\n");

        if (getMetadata() != null) {
            for (NameValuePair nvp : getMetadata()) {
                sb.append("Meta: " + nvp.getName() + " Value: " + nvp.getStringValue() + "\n");
            }
        }

        return super.toString() + sb.toString();
    }
}
