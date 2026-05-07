package com.flightstream.consumer.configuration;

import com.flightstream.consumer.handler.SocketConnectionHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;


@Configuration
@EnableWebSocket
public class WebSocketConfigConfiguration implements WebSocketConfigurer {
    final private SocketConnectionHandler socketConnectionHandler;

    public WebSocketConfigConfiguration(SocketConnectionHandler socketConnectionHandler){
        this.socketConnectionHandler=socketConnectionHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(socketConnectionHandler,"/alerts").setAllowedOrigins("*");
    }
}

