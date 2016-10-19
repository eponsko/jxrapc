package com.wpl.xrapc;

import java.io.DataOutputStream;
import java.io.IOException;

public class XrapGetEmptyReply extends XrapReply {
    public XrapGetEmptyReply(int requestId, short statusCode) {
        this.setRequestId(requestId);
        this.setStatusCode(statusCode);
    }

    public XrapGetEmptyReply() {
    }

    public void buildReply(DataOutputStream dos) throws IOException {
        dos.writeShort(Constants.SIGNATURE);
        dos.writeByte(Constants.GET_EMPTY_COMMAND);
        dos.writeInt(getRequestId());
        dos.writeShort(getStatusCode());
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Type: GET-EMPTY\n");

        return super.toString() + sb.toString();
    }
}
