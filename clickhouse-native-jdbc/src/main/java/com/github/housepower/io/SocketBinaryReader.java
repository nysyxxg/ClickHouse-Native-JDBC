/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.housepower.io;

import com.github.housepower.misc.NettyUtil;
import com.github.housepower.settings.ClickHouseDefines;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.Socket;

public class SocketBinaryReader implements BinaryReader {

    private final InputStream in;
    private final ByteBuf buf;

    public SocketBinaryReader(Socket socket) throws IOException {
        this(socket.getInputStream(), ClickHouseDefines.SOCKET_RECV_BUFFER_BYTES);
    }

    SocketBinaryReader(InputStream in, int capacity) {
        this.in = in;
        this.buf = NettyUtil.alloc().buffer(capacity, capacity);
    }

    @Override
    public int readByte() {
        maybeRefill(1);
        return buf.readByte() & 0xFF;
    }

    @Override
    public int readBytes(byte[] bytes) {
        for (int i = 0; i < bytes.length; ) {
            maybeRefill(bytes.length);
            int writableBytes = bytes.length - i;
            int len = Math.min(writableBytes, buf.readableBytes());
            if (len > 0) {
                buf.readBytes(bytes, i, len);
                i += len;
            }
        }
        return bytes.length;
    }

    @Override
    public void close() {
        ReferenceCountUtil.safeRelease(buf);
    }

    private void maybeRefill(int atLeastReadableBytes) {
        if (buf.isReadable(atLeastReadableBytes))
            return;
        try {
            ByteBuf remaining = null;
            if (buf.isReadable()) {
                int remainingLen = buf.readableBytes();
                remaining = NettyUtil.alloc().buffer(remainingLen, remainingLen);
                buf.readBytes(remaining);
            }
            buf.clear();
            if (remaining != null) {
                buf.writeBytes(remaining);
                ReferenceCountUtil.safeRelease(remaining);
            }
            int n = buf.writeBytes(in, buf.writableBytes());
            if (n <= 0) {
                throw new UncheckedIOException(new EOFException("Attempt to read after eof."));
            }
        } catch (IOException rethrow) {
            throw new UncheckedIOException(rethrow);
        }
    }
}
