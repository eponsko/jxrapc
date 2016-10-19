package com.wpl.xrapc;

/**
 * Represents a particular resource, and the different operations that can be used on it
 * Created by eponsko on 2016-09-21.
 */

public interface XrapResourceService {

    String getRoute();

    XrapReply GET(XrapGetRequest request);

    XrapReply DELETE(XrapDeleteRequest request);

    XrapReply POST(XrapPostRequest request);

    XrapReply PUT(XrapPutRequest request);
}
