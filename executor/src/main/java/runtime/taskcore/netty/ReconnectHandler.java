package runtime.taskcore.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class ReconnectHandler extends ChannelInboundHandlerAdapter {

    private final Bootstrap bootstrap;

    public ReconnectHandler(Bootstrap bootstrap) {
        this.bootstrap = bootstrap;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        System.out.println("Disconnected from the server, starting reconnect...");
        final EventLoop eventLoop = ctx.channel().eventLoop();
        eventLoop.schedule(() -> {
            System.out.println("Reconnecting to the server...");
            connectToServer();
        }, 5L, java.util.concurrent.TimeUnit.SECONDS);
    }

    private void connectToServer() {
        ChannelFuture future = bootstrap.connect();
        future.addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                System.out.println("Failed to connect, retrying...");
                final EventLoop eventLoop = f.channel().eventLoop();
                eventLoop.schedule(this::connectToServer, 5L, java.util.concurrent.TimeUnit.SECONDS);
            } else {
                System.out.println("Connected to the server.");
            }
        });
    }



    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}

