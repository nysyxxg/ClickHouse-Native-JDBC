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
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.Socket;

public class SocketBinaryWriter implements BinaryWriter {

    private final OutputStream out;
    private final ByteBuf buf;

    public SocketBinaryWriter(Socket socket) throws IOException {
        this(socket.getOutputStream());
    }

    SocketBinaryWriter(OutputStream output) {
        this.out = output;
        this.buf = NettyUtil.alloc().buffer();
    }

    @Override
    public void writeByte(byte byt) {
        buf.writeByte(byt);
    }

    @Override
    public void writeBytes(byte[] bytes) {
        buf.writeBytes(bytes);
    }

    @Override
    public void writeBytes(byte[] bytes, int offset, int length) {
        buf.writeBytes(bytes, offset, length);
    }

    @Override
    public void flush(boolean force) {
        try {
            buf.readBytes(out, buf.readableBytes());
            buf.clear();
            out.flush();
        } catch (IOException rethrow) {
            throw new UncheckedIOException(rethrow);
        }
    }
}
