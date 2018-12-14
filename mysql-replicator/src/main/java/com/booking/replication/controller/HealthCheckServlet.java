package com.booking.replication.controller;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class HealthCheckServlet extends HttpServlet {

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws  IOException
    {
        response.setContentType("text/json");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("ok");
    }
}
