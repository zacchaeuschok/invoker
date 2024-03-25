package runtime.taskcore.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import runtime.taskcore.KeyValuePair;

import java.util.Queue;

public class KeyValueProcessingHandler extends SimpleChannelInboundHandler<KeyValuePair> {
    Queue<KeyValuePair> queue;
    public KeyValueProcessingHandler(Queue<KeyValuePair> queue) {
        this.queue = queue;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, KeyValuePair msg) {
        System.out.println("Received key: " + msg.key + ", value: " + msg.value);
        queue.offer(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
