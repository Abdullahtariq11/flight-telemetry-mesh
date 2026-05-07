package com.flightstream.consumer.handler;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class SocketConnectionHandler extends TextWebSocketHandler {

    List<WebSocketSession> socketSessions= Collections.synchronizedList(new ArrayList<>());


    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        super.afterConnectionEstablished(session);
        System.out.println(session.getId() + " connected");
        socketSessions.add(session);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        super.afterConnectionClosed(session, status);
        System.out.println(session.getId() + " Disconnected");
        socketSessions.remove(session);
    }

    public void sendAlert(String message) throws IOException {
        for(WebSocketSession session:socketSessions ){
            if(session.isOpen()){
                session.sendMessage(new TextMessage(message));
            }
        }

    }

}
