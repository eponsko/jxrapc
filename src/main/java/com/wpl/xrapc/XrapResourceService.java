package com.wpl.xrapc;

/**
 * Represents a particular resource, and the different operations that can be used on it
 * Created by eponsko on 2016-09-21.
 */

public interface XrapResourceService {

    public String getRoute();
    public XrapReply GET(XrapGetRequest request) ;
    public XrapReply DELETE(XrapDeleteRequest request);
    public XrapReply POST(XrapPostRequest request);
    public XrapReply PUT(XrapPutRequest request);
}
