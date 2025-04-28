package org.apache.kafka.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * @author gbl.huang
 * @date 2025/04/27 20:48
 **/
public class Demo {
    public static void main(String[] args) throws IOException {
        // 创建 selector, 管理多个 channel
        Selector selector = Selector.open();

        // 创建 ServerSocketChannel 并且绑定端口
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.bind(new InetSocketAddress(8080));

        // 将 channel 注册到 selector 上
        SelectionKey serverSocketChannelKey = serverSocketChannel.register(selector, 0);
        // 由于总共有 4 种事件, 分别是 accept、connect、read 和 write,
        // 分别代表有连接请求时触发、客户端建立连接时触发、可读事件、可写事件
        // 我们可以使用 interestOps 来表明只处理有连接请求的事件
        serverSocketChannelKey.interestOps(SelectionKey.OP_ACCEPT);

        System.out.printf("serverSocketChannel %s\n", serverSocketChannelKey);
        while (true) {
            // 没有事件发生, 线程会阻塞; 有事件发生, 就会让线程继续执行
            System.out.println("start to select...");
            selector.select(1000L);
            // 换句话说, 有连接过来了, 就会继续往下走

            // 通过 selectedKeys 包含了所有发生的事件, 可能会包含 READ 或者 WRITE
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                System.out.printf("selected key %s\n", key);

                // 这里需要进行事件区分
                if (key.isAcceptable()) {
                    System.out.println("get acceptable event");

                    // 触发此次事件的 channel, 拿到事件一定要处理, 否则会进入非阻塞模式, 空转占用 CPU
                    // 例如你可以使用 key.cancel()
                    ServerSocketChannel channel = (ServerSocketChannel) key.channel();
                    SocketChannel socketChannel = channel.accept();
                    socketChannel.configureBlocking(false);

                    // 这个 socketChannel 也需要注册到 selector 上, 相当于把控制权交给 selector
                    SelectionKey socketChannelKey = socketChannel.register(selector, 0);
                    socketChannelKey.interestOps(SelectionKey.OP_READ);
                    System.out.printf("get socketChannel %s\n", socketChannel);
                } else if (key.isReadable()) {
                    System.out.println("get readable event");

                    SocketChannel channel = (SocketChannel) key.channel();
                    ByteBuffer buf = ByteBuffer.allocate(16);
                    channel.read(buf);
                    buf.flip();
//                    ByteBufferUtil.debugRead(buf);
                    key.cancel();
                }

                iterator.remove();
            }
        }
    }
}