package com.wpl.xrapc;

/**
 * Represents a particular resource, and the different operations that can be used on it
 * Created by eponsko on 2016-09-21.
 */

public class XrapResource implements XrapResourceService{
    private String route;

    public XrapResource() {
        setRoute("/");
    }

    public XrapResource(String route) {
        setRoute(route);
    }

    public String getRoute() {
        return route;
    }

    public void setRoute(String route) {
        this.route = route;
    }

    public XrapReply GET(XrapGetRequest request) {
        XrapErrorReply rep = new XrapErrorReply();
        rep.setErrorText("Not Implemented");
        rep.setStatusCode(Constants.NotImplemented_501);
        rep.setRequestId(request.getRequestId());
        rep.setRouteid(request.getRouteid());
        return rep;
    }

    public XrapReply DELETE(XrapDeleteRequest request) {
        XrapErrorReply rep = new XrapErrorReply();
        rep.setErrorText("Not Implemented");
        rep.setStatusCode(Constants.NotImplemented_501);
        rep.setRequestId(request.getRequestId());
        rep.setRouteid(request.getRouteid());
        return rep;
    }

    public XrapReply POST(XrapPostRequest request) {
        XrapErrorReply rep = new XrapErrorReply();
        rep.setErrorText("Not Implemented");
        rep.setStatusCode(Constants.NotImplemented_501);
        rep.setRequestId(request.getRequestId());
        rep.setRouteid(request.getRouteid());
        return rep;
    }

    public XrapReply PUT(XrapPutRequest request) {
        XrapErrorReply rep = new XrapErrorReply();
        rep.setErrorText("Not Implemented");
        rep.setStatusCode(Constants.NotImplemented_501);
        rep.setRequestId(request.getRequestId());
        rep.setRouteid(request.getRouteid());
        return rep;
    }
}
