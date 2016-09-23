package com.wpl.xrapc;

public class Constants {
	public static final short SIGNATURE = (short)((0xAAA5)&0xffff);
	public static final int POST_COMMAND = 1;
	public static final int POST_OK_COMMAND = 2;
	public static final int GET_COMMAND = 3;
	public static final int GET_OK_COMMAND = 4;
	public static final int GET_EMPTY_COMMAND = 5;
	public static final int PUT_COMMAND = 6;
	public static final int PUT_OK_COMMAND = 7;
	public static final int DELETE_COMMAND = 8;
	public static final int DELETE_OK_COMMAND = 9;
	public static final int ERROR_COMMAND = 10;

	/* POST
    200 OK - the resource already existed as specified (only possible for public resources).
    201 Created - the server created the resource, and the Location: header provides the URN.
    400 Bad Request - the resource specification was incomplete or badly formatted.
    403 Forbidden - the POST method is not allowed on the parent URN.
    */

	/* GET
    200 OK - the resource already existed as specified (only for public resources).
    304 Not Modified - the client already has the latest copy of the resource.
	*/

	/* PUT
    200 OK - the resource was successfully updated.
    204 No Content - the client provided no resource document and the update request had no effect.
    400 Bad Request - the resource document was incomplete or badly formatted.
    403 Forbidden - the PUT method is not allowed on the resource.
    412 Precondition Failed - the client does not have the latest copy of the resource.
    */

	/* DELETE
	200 OK - the resource was successfully updated.
    403 Forbidden - the DELETE method is not allowed on the resource.
    412 Precondition Failed - the client does not have the latest copy of the resource.
	*/

	public static final short OK_200 = 200;
	public static final short Created_201 = 201;
	public static final short NoContent_204 = 204;
	public static final short NotModfied_304 = 304;
	public static final short BadRequest_400 = 400;
	public static final short Unauthorized_401 = 401;
	public static final short Forbidden_403 = 403;
	public static final short NotFound_404 = 404;
	public static final short PreconditionFailed_412 = 412;
	public static final short TooLarge_413 = 413;
	public static final short InternalError_500 = 500;
	public static final short NotImplemented_501 = 501;
	public static final short Overloaded_503 = 503;

	public static String StatusCodeToString(short statusCode){
		switch (statusCode){
			case OK_200:
				return "200 OK";
			case Created_201:
				return "201 Created";
			case NoContent_204:
				return "204 No Content";
			case NotModfied_304:
				return "304 Not Modified";
			case BadRequest_400:
				return "400 Bad Request";
			case Unauthorized_401:
				return "401 Unauthorized";
			case Forbidden_403:
				return "403 Forbidden";
			case NotFound_404:
				return "404 Not Found";
			case PreconditionFailed_412:
				return "412 Precondition Failed";
			case TooLarge_413:
				return "413 Too Large";
			case InternalError_500:
				return "500 Internal Error";
			case NotImplemented_501:
				return "501 Not Implemented";
			case Overloaded_503:
				return "503 Overloaded";
			default:
				return "Unknown status code: " + statusCode;
		}
	}
}
