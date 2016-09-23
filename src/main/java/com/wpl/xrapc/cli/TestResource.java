package com.wpl.xrapc.cli;
/*
attempt to use Jackson for JSON/XML marshalling
painful
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
*/
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

/* attempt to use JAXB for XML marshalling
painful

import org.eclipse.persistence.jaxb.MarshallerProperties;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.*;
*/

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.wpl.xrapc.*;


import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/*
 TestResource
 Represents the "/test" resource
 PUT/POST/DELETE not implemented, will use the inherited versions

 */


class Link {
    public Link(String src, String dst){
        source = src;
        destination = dst;
    }
    private String source = null;
    private String destination = null;

    @Override
    public String toString() {
        return "Link{Source: " + source + " Destination: " + destination + "}";
    }
}

public class TestResource extends XrapResource {
    private Logger log;
    HashMap<Integer, Link> testMap;

    public TestResource() {
        testMap = new HashMap<>();
        log = LoggerFactory.getLogger(TestResource.class);
        setRoute("/test/");
        testMap.put(1, new Link("apa", "gapa"));
        testMap.put(2, new Link("hatt", "fnatt"));
    }

    public XrapReply POST(XrapPostRequest request){
        boolean valid = true;
        int length;
        if(request.getContentType() != null)
            if (request.getContentType().equals("application/json"))
                valid = true;
            else
                valid = false;
        else
            valid = false;

        if(!valid){
            log.info("Bad contentType!");
            XrapErrorReply rep = new XrapErrorReply();
            rep.setStatusCode(Constants.BadRequest_400);
            rep.setRouteid(request.getRouteid());
            rep.setRequestId(request.getRequestId());
            rep.setErrorText("Only ContentType: application/json supported");
            return rep;
        }


        String json = new String(request.getContentBody());
        Gson gson = new GsonBuilder().create();
        Link l = gson.fromJson(json,Link.class);
        log.info("deserialised to " + l );
        length = testMap.size() + 1;
        testMap.put(length, l);

        XrapPutReply rep = new XrapPutReply();
        rep.setEtag(new Integer(length).toString());
        rep.setDateModified(new Date().getTime());
        rep.setStatusCode(Constants.Created_201);
        rep.setLocation("/text/"+length);
        rep.setRequestId(request.getRequestId());
        rep.setRouteid(request.getRouteid());
        return rep;

    }

    private static Pattern GET_PATTERN = Pattern.compile("/test/(.+)");
    @Override
    public XrapReply GET(XrapGetRequest request) {
        String retval = "10";
        Integer index = null;
        Matcher m;
        Link res = null;
        boolean useXml = false;
        String replyBody = "";
        Gson gson = new GsonBuilder().create();

        // support subresources by parameter "id"
        // or /test/{id}
        String location = request.getResource();
        m = GET_PATTERN.matcher(location);
        if (m.matches()) {
            index = new Integer(m.group(1));
            res = testMap.get(index);
        }

        for (XrapGetRequest.Parameter p : request.getParameters()) {
            if (p.getName().equals("id")) {
                // prefer the parameter in the URN
                if (index == null) {
                    index = new Integer(p.getStringValue());
                    res = testMap.get(index);
                }
            } else {
                // unknown parameter passed, lets disagree with that
                log.info("Unknown parameter " + p.getName());
                XrapErrorReply rep = new XrapErrorReply();
                rep.setStatusCode(Constants.BadRequest_400);
                rep.setRouteid(request.getRouteid());
                rep.setRequestId(request.getRequestId());
                rep.setErrorText("Unknown parameter \"" + p.getName() + "\"");
                return rep;
            }
        }

        if (request.getContentType() != null) {
            if (request.getContentType().equals("application/xml")) {
                useXml = true;
            }
        }
            if (res == null)
                replyBody = gson.toJson(testMap, HashMap.class);
            else
                replyBody = gson.toJson(res,Link.class);

        if (replyBody != null) {
            XrapGetReply rep = new XrapGetReply();
            if(useXml)
                rep.setContentType("application/test+xml");
            else
                rep.setContentType("application/test+json");
            rep.setBody(replyBody.getBytes());

            if(index != null) {
                rep.setEtag(index.toString());
            }else {
                rep.setEtag("*");
            }
            rep.setDateModified(new Date().getTime());
            rep.setStatusCode(Constants.OK_200);
            rep.setRequestId(request.getRequestId());
            rep.setRouteid(request.getRouteid());
            return rep;
        } else {
            log.info("Replying with 404");
            XrapErrorReply rep = new XrapErrorReply();
            rep.setStatusCode(Constants.NotFound_404);
            rep.setRouteid(request.getRouteid());
            rep.setRequestId(request.getRequestId());
            rep.setErrorText("Could not find resource");
            return rep;
        }
    }
}