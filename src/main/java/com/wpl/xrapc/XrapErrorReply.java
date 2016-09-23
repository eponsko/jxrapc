package com.wpl.xrapc;

import java.io.DataOutputStream;
import java.io.IOException;

public class XrapErrorReply extends XrapReply {
    private String errorText;

    public XrapErrorReply(int requestId, short statusCode, String errorText) {
        setRequestId(requestId);
        setStatusCode(statusCode);
        setErrorText(errorText);
    }

    public XrapErrorReply() {
    }

    void buildReply(DataOutputStream dos) throws IOException {
        dos.writeShort(Constants.SIGNATURE);
        dos.writeByte(Constants.ERROR_COMMAND);
        dos.writeInt(getRequestId());
        dos.writeShort(getStatusCode());
        writeString(dos, getErrorText());
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ErrorText: " + errorText + "\n");
        sb.append("Type: ERROR\n");
        return super.toString() + sb.toString();
    }

    public String getErrorText() {
        return errorText;
    }


    public void setErrorText(String errorText) {
        this.errorText = errorText;
    }
}

