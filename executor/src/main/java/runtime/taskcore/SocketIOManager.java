package runtime.taskcore;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import runtime.taskcore.api.IOManager;
import runtime.taskcore.netty.KeyValueProcessingHandler;
import runtime.taskcore.netty.LengthFieldBasedDecoder;
import runtime.taskcore.netty.ReconnectHandler;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;


public class SocketIOManager implements IOManager {

    private Socket inputSocket;
    private Socket outputSocket;


    private String hostname = "localhost";
    private int inputPort = 50001;
    private InputStream in;
    private OutputStream out;
    private final int maxRetries = 5;

    private final long waitTimeInMillis = 10000;

    private int outputPort = 50002;

    private final Queue<KeyValuePair> messageQueue = new ConcurrentLinkedQueue<>();

    private boolean reconnecting = false;

    public SocketIOManager() {
        connectInput();
        connectOutput();
    }

    public void connectInput() {
        try {
            Bootstrap b = new Bootstrap();
            b.group(new NioEventLoopGroup())
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new LengthFieldBasedDecoder(), new KeyValueProcessingHandler(messageQueue));
                            ch.pipeline().addLast(new ReconnectHandler(b));
                        }
                    });
            b.connect("localhost", 50001).sync();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void connectOutput() {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                this.outputSocket = new Socket(hostname, outputPort);
                this.out = outputSocket.getOutputStream();
                System.out.println("Output Connection established");
                reconnecting = false;
                break;
            } catch (IOException e) {
                System.out.println(e);
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(waitTimeInMillis);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        System.out.println("Thread interrupted: " + ie);
                        break;
                    }
                }
            }
        }
    }

    /**
     * Get the next batch of records by polling.
     *
     * @param pollTime how long to block in Consumer#poll
     * @return Next batch of records or null if no records available.
     */
    @Override
    public List<KeyValuePair> pollRequests(final Duration pollTime) {
        List<KeyValuePair> records = new ArrayList<>();
        while (!messageQueue.isEmpty()) {
            records.add(messageQueue.poll());
        }

        return records;
    }

    private int bytesToInt(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return bb.getInt();
    }

    @Override
    public void send(KeyValuePair data) {
        try {
            byte[] key = data.key.getBytes("UTF-8");

            out.write(intToBytes(key.length)); // Send the length of the key
            if (key.length != 0) {
                out.write(key);
            }
            System.out.println("sent key: " + data.key);
            byte[] value = data.value.getBytes("UTF-8");
            out.write(intToBytes(value.length)); // Send the length of the value
            if (value.length != 0) {
                out.write(value);
            }
            System.out.println("sent val: " + data.value);

            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private byte[] intToBytes(int i) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(i);
        return bb.array();
    }


}
