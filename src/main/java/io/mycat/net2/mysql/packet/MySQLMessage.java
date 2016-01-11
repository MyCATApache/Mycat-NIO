/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.net2.mysql.packet;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import io.mycat.net2.ByteBufferArray;

/**
 * @author mycat
 */
public class MySQLMessage {
    public static final long NULL_LENGTH = -1;
    private static final byte[] EMPTY_BYTES = new byte[0];

    // private final byte[] data;
    private final int length;
    private int position;
    private final ByteBufferArray bufferArray;
    private final int packetIndex;

    public MySQLMessage(ByteBufferArray bufferArray, int packetIndex) {
        this.bufferArray = bufferArray;
        this.length = bufferArray.getPacageLength(packetIndex);
        this.position = 0;
        this.packetIndex = packetIndex;
    }

    public int length() {
        return length;
    }

    public int position() {
        return position;
    }

    // public byte[] bytes() {
    // return data;
    // }

    public void move(int i) {
        position += i;
    }

    public void position(int i) {
        this.position = i;
    }

    public boolean hasRemaining() {
        return length > position;
    }

    public byte read(int i) {
        return bufferArray.readPacket(packetIndex, i);
    }

    public byte read() {
        return bufferArray.readPacket(packetIndex, position++);
    }

    public int readUB2() {
        int i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        return i;
    }

    public int readUB3() {
        int i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        i |= (read() & 0xff) << 16;
        return i;
    }

    public long readUB4() {
        long l = (long) (read() & 0xff);
        l |= (long) (read() & 0xff) << 8;
        l |= (long) (read() & 0xff) << 16;
        l |= (long) (read() & 0xff) << 24;
        return l;
    }

    public int readInt() {
        int i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        i |= (read() & 0xff) << 16;
        i |= (read() & 0xff) << 24;
        return i;
    }

    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    public long readLong() {
        long l = (long) (read() & 0xff);
        l |= (long) (read() & 0xff) << 8;
        l |= (long) (read() & 0xff) << 16;
        l |= (long) (read() & 0xff) << 24;
        l |= (long) (read() & 0xff) << 32;
        l |= (long) (read() & 0xff) << 40;
        l |= (long) (read() & 0xff) << 48;
        l |= (long) (read() & 0xff) << 56;
        return l;
    }

    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    public long readLength() {
        int length = read() & 0xff;
        switch (length) {
        case 251:
            return NULL_LENGTH;
        case 252:
            return readUB2();
        case 253:
            return readUB3();
        case 254:
            return readLong();
        default:
            return length;
        }
    }

    private void arraycopy(byte[] dest) {
        arraycopy(dest, dest.length);
    }

    private void arraycopy(byte[] dest, int length) {
        for (int i = 0; i < length; i++) {
            dest[i] = read(position + i);
        }
    }

    public byte[] readBytes() {
        if (position >= length) {
            return EMPTY_BYTES;
        }
        byte[] ab = new byte[length - position];
        // System.arraycopy(data, position, ab, 0, ab.length);
        arraycopy(ab);
        position = length;
        return ab;
    }

    public byte[] readBytes(int length) {
        byte[] ab = new byte[length];
        // System.arraycopy(data, position, ab, 0, length);
        arraycopy(ab, length);
        position += length;
        return ab;
    }

    public byte[] readBytesWithNull() {
        if (position >= length) {
            return EMPTY_BYTES;
        }
        int offset = -1;
        for (int i = position; i < length; i++) {
            if (read(i) == 0) {
                offset = i;
                break;
            }
        }
        switch (offset) {
        case -1:
            byte[] ab1 = new byte[length - position];
            // System.arraycopy(b, position, ab1, 0, ab1.length);
            arraycopy(ab1);
            position = length;
            return ab1;
        case 0:
            position++;
            return EMPTY_BYTES;
        default:
            byte[] ab2 = new byte[offset - position];
            // System.arraycopy(b, position, ab2, 0, ab2.length);
            arraycopy(ab2);
            position = offset + 1;
            return ab2;
        }
    }

    public byte[] readBytesWithLength() {
        int length = (int) readLength();
        if (length == NULL_LENGTH) {
            return null;
        }
        if (length <= 0) {
            return EMPTY_BYTES;
        }

        byte[] ab = new byte[length];
        // System.arraycopy(data, position, ab, 0, ab.length);
        arraycopy(ab);
        position += length;
        return ab;
    }

    public String readString() {
        if (position >= length) {
            return null;
        }
        byte[] ab = new byte[length - position];
        arraycopy(ab);
        String s = new String(ab);
        position = length;
        return s;
    }

    public String readString(String charset) throws UnsupportedEncodingException {
        if (position >= length) {
            return null;
        }
        byte[] ab = new byte[length - position];
        arraycopy(ab);
        String s = new String(ab, charset);
        position = length;
        return s;
    }

    public String readStringWithNull() {
        if (position >= length) {
            return null;
        }
        int offset = -1;
        for (int i = position; i < length; i++) {
            if (read(i) == 0) {
                offset = i;
                break;
            }
        }
        if (offset == -1) {
            byte[] ab = new byte[length - position];
            arraycopy(ab);
            String s = new String(ab);
            position = length;
            return s;
        }
        if (offset > position) {
            byte[] ab = new byte[offset - position];
            arraycopy(ab);
            String s = new String(ab);
            position = offset + 1;
            return s;
        } else {
            position++;
            return null;
        }
    }

    public String readStringWithNull(String charset) throws UnsupportedEncodingException {
        if (position >= length) {
            return null;
        }
        int offset = -1;
        for (int i = position; i < length; i++) {
            if (read(i) == 0) {
                offset = i;
                break;
            }
        }
        switch (offset) {
        case -1:
            byte[] ab = new byte[length - position];
            arraycopy(ab);
            String s1 = new String(ab, charset);
            position = length;
            return s1;
        case 0:
            position++;
            return null;
        default:
            byte[] b = new byte[offset - position];
            arraycopy(b);
            String s2 = new String(b, charset);
            position = offset + 1;
            return s2;
        }
    }

    public String readStringWithLength() {
        int length = (int) readLength();
        if (length <= 0) {
            return null;
        }
        byte[] ab = new byte[length - position];
        arraycopy(ab);
        String s = new String(ab);
        position += length;
        return s;
    }

    public String readStringWithLength(String charset) throws UnsupportedEncodingException {
        int length = (int) readLength();
        if (length <= 0) {
            return null;
        }
        byte[] ab = new byte[length - position];
        arraycopy(ab);
        String s = new String(ab, charset);
        position += length;
        return s;
    }

    public java.sql.Time readTime() {
        move(6);
        int hour = read();
        int minute = read();
        int second = read();
        Calendar cal = getLocalCalendar();
        cal.set(0, 0, 0, hour, minute, second);
        return new Time(cal.getTimeInMillis());
    }

    public java.util.Date readDate() {
        byte length = read();
        int year = readUB2();
        byte month = read();
        byte date = read();
        int hour = read();
        int minute = read();
        int second = read();
        if (length == 11) {
            long nanos = readUB4();
            Calendar cal = getLocalCalendar();
            cal.set(year, --month, date, hour, minute, second);
            Timestamp time = new Timestamp(cal.getTimeInMillis());
            time.setNanos((int) nanos);
            return time;
        } else {
            Calendar cal = getLocalCalendar();
            cal.set(year, --month, date, hour, minute, second);
            return new java.sql.Date(cal.getTimeInMillis());
        }
    }

    public BigDecimal readBigDecimal() {
        String src = readStringWithLength();
        return src == null ? null : new BigDecimal(src);
    }

    public String toString() {
        return bufferArray.toString();
    }

    private static final ThreadLocal<Calendar> localCalendar = new ThreadLocal<Calendar>();

    private static final Calendar getLocalCalendar() {
        Calendar cal = localCalendar.get();
        if (cal == null) {
            cal = Calendar.getInstance();
            localCalendar.set(cal);
        }
        return cal;
    }

}