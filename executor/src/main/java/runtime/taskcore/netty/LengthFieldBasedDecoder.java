package runtime.taskcore.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import runtime.taskcore.KeyValuePair;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class LengthFieldBasedDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)  {
        if (in.readableBytes() < 4) return;

        in.markReaderIndex();

        int keyLength = in.readInt();
        if (in.readableBytes() < keyLength) {
            in.resetReaderIndex();
            return;
        }

        ByteBuf keyBuf = in.readBytes(keyLength); // Read the key
        String key = keyBuf.toString(StandardCharsets.UTF_8);

        if (in.readableBytes() < 4) return; // Length of the value
        int valueLength = in.readInt();
        if (in.readableBytes() < valueLength) {
            in.resetReaderIndex(); // Reset to the marked index if not enough data
            return;
        }

        ByteBuf valueBuf = in.readBytes(valueLength); // Read the value
        String value = valueBuf.toString(StandardCharsets.UTF_8);

        out.add(new KeyValuePair(key, value)); // Add the key-value pair to the list
    }
}
