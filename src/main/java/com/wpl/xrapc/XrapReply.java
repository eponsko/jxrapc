package com.wpl.xrapc;

import java.io.DataOutputStream;
import java.io.IOException;


public abstract class XrapReply extends XrapMessage {
    private short statusCode;

    abstract void buildReply(DataOutputStream dos) throws IOException;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("StatusCode: " + Constants.StatusCodeToString(getStatusCode()) + "\n");
        return super.toString() + sb.toString();
    }

    public short getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(short statusCode) {
        this.statusCode = statusCode;
    }
}

