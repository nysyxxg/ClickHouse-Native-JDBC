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

package com.github.housepower.serde;

import com.github.housepower.io.BinaryReader;
import com.github.housepower.io.DecompressBinaryReader;
import com.github.housepower.misc.Switcher;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

public class LegacyBinaryDeserializer implements BinaryDeserializer, SupportCompress {

    private final Switcher<BinaryReader> switcher;
    private final boolean enableCompress;

    public LegacyBinaryDeserializer(BinaryReader buffedReader, boolean enableCompress) {
        this.enableCompress = enableCompress;
        BinaryReader compressedReader = null;
        if (enableCompress) {
            compressedReader = new DecompressBinaryReader(buffedReader);
        }
        switcher = new Switcher<>(compressedReader, buffedReader);
    }

    public long readVarInt() {
        long number = 0;
        for (int i = 0; i < 9; i++) {
            int byt = switcher.get().readByte();

            number |= (long) (byt & 0x7F) << (7 * i);

            if ((byt & 0x80) == 0) {
                break;
            }
        }
        return number;
    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public short readShortLE() {
        // @formatter:off
        return (short) (((switcher.get().readByte() & 0xFF) << 0)
                      + ((switcher.get().readByte() & 0xFF) << 8));
        // @formatter:on
    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public int readIntLE() {
        // @formatter:off
        return ((switcher.get().readByte() & 0xFF) << 0)
             + ((switcher.get().readByte() & 0xFF) << 8)
             + ((switcher.get().readByte() & 0xFF) << 16)
             + ((switcher.get().readByte() & 0xFF) << 24);
        // @formatter:on
    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public long readLongLE() {
        // @formatter:off
        return ((switcher.get().readByte() & 0xFFL) << 0)
             + ((switcher.get().readByte() & 0xFFL) << 8)
             + ((switcher.get().readByte() & 0xFFL) << 16)
             + ((switcher.get().readByte() & 0xFFL) << 24)
             + ((switcher.get().readByte() & 0xFFL) << 32)
             + ((switcher.get().readByte() & 0xFFL) << 40)
             + ((switcher.get().readByte() & 0xFFL) << 48)
             + ((switcher.get().readByte() & 0xFFL) << 56);
        // @formatter:on
    }

    public boolean readBoolean() {
        return (switcher.get().readByte() != 0);
    }

    public ByteBuf readBytesBinary() {
        byte[] data = new byte[(int) readVarInt()];
        switcher.get().readBytes(data);
        return Unpooled.wrappedBuffer(data);
    }

    public String readUTF8Binary() {
        byte[] data = new byte[(int) readVarInt()];
        return switcher.get().readBytes(data) > 0 ? new String(data, StandardCharsets.UTF_8) : "";
    }

    public byte readByte() {
        return (byte) switcher.get().readByte();
    }

    public void maybeEnableCompressed() {
        if (enableCompress) {
            switcher.select(false);
        }
    }

    public void maybeDisableCompressed() {
        if (enableCompress) {
            switcher.select(true);
        }
    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public float readFloatLE() {
        // @formatter:off
        return Float.intBitsToFloat(
                ((switcher.get().readByte() & 0xFF) << 0)
              + ((switcher.get().readByte() & 0xFF) << 8)
              + ((switcher.get().readByte() & 0xFF) << 16)
              + ((switcher.get().readByte()       ) << 24));
        // @formatter:on
    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public double readDoubleLE() {
        // @formatter:off
        return Double.longBitsToDouble(
                ((switcher.get().readByte() & 0xFFL) << 0 )
              + ((switcher.get().readByte() & 0xFFL) << 8 )
              + ((switcher.get().readByte() & 0xFFL) << 16)
              + ((switcher.get().readByte() & 0xFFL) << 24)
              + ((switcher.get().readByte() & 0xFFL) << 32)
              + ((switcher.get().readByte() & 0xFFL) << 40)
              + ((switcher.get().readByte() & 0xFFL) << 48)
              + ((switcher.get().readByte() & 0xFFL) << 56)
        );
        // @formatter:on
    }

    public ByteBuf readBytes(int size) {
        byte[] bytes = new byte[size];
        switcher.get().readBytes(bytes);
        return Unpooled.wrappedBuffer(bytes);
    }

    @Override
    public void close() {
        switcher.get().close();
    }
}
