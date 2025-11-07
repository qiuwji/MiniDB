package com.qiu.net;

public class Response {

    private boolean success;
    private byte[] data;
    private String message;

    private Response(boolean success, byte[] data, String message) {
        this.success = success;
        this.data = data;
        this.message = message;
    }

    public static Response fail(String message) {
        return new Response(false, null, message);
    }

    public static Response success(String message, byte[] data) {
        return new Response(true, data, message);
    }
}
