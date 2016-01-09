package io.mycat.net2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import junit.framework.Assert;

public class TestMySQLPackageSplitter {

    /**
     * 创建10个随机大小的报文，测试报文内容拆分后能否正确读取
     * 
     * @throws IOException
     */
    @Test
    public void testSpliter() throws IOException {
        ByteBufferArray bufArray = splitPackage(createRandomMysgqlPackages(10), 10240);
        Assert.assertEquals(10, bufArray.getCurPacageIndex());
    }

    private ByteBufferArray splitPackage(ByteArrayInputStream mysqlPackgsStream, int chunkSize) throws IOException {
        SharedBufferPool sharedPool = new SharedBufferPool(1024 * 1024 * 100, chunkSize);
        ReactorBufferPool reactBufferPool = new ReactorBufferPool(sharedPool, Thread.currentThread(), 1000);
        ByteBufferArray bufArray = reactBufferPool.allocate();
        bufArray.addNewBuffer();
        int readBufferOffset = 0;
        // @todo 构造符合MYSQL报文的数据包，进行测试

        while (mysqlPackgsStream.available() > 0) {
            ByteBuffer curBuf = bufArray.getLastByteBuffer();
            if (!curBuf.hasRemaining()) {
                curBuf = bufArray.addNewBuffer();
            }
            byte[] data = new byte[curBuf.remaining()];
            int readed = mysqlPackgsStream.read(data);
            curBuf.put(data, 0, readed);
            readBufferOffset = CommonPackageUtil.parsePackages(bufArray, curBuf, readBufferOffset);
        }
        return bufArray;
    }

    /**
     * 随机构造报文进行测试，报文长度按照报文协议特点，在0xFF-0xFFFF-0xFFFFFF上均匀分布
     * 
     * @return
     * @throws IOException
     */
    private ByteArrayInputStream createRandomMysgqlPackages(int packagesCount) throws IOException {
        List<Integer> lenList = new ArrayList<>();
        for (int i = 0; i < packagesCount; i++) {
            switch ((int) (Math.random() * 3)) {
            case 0:
                lenList.add((int) (Math.random() * 0xFF));
                break;
            case 1:
                lenList.add((int) (Math.random() * 0xFFFF));
                break;
            case 2:
                lenList.add((int) (Math.random() * 0xFFFFFF));
                break;
            }

        }
        return createMultiPackage(lenList);
    }

    /**
     * 根据MySQL报文格式构造内容长度为len的报文（总长度应为len+4）
     * 
     * @param len
     *            内容长度
     * @return 报文字节数组
     */
    private byte[] createPackage(int len) {
        byte[] p = new byte[len + 4];
        p[0] = (byte) (len & 0xFF);
        p[1] = (byte) ((len >> 8) & 0xFF);
        p[2] = (byte) ((len >> 16) & 0xFF);
        for (int i = 0; i < len; i++) {
            p[4 + i] = (byte) i;
        }
        return p;
    }

    /**
     * 创建4个报文，第2、4个报文Header被拆分，测试能否读到正确长度
     * 
     * @throws IOException
     */
    @Test
    public void testHeaderSpliter() throws IOException {
        ByteBufferArray bufArray = splitPackage(createMultiPackage(Arrays.asList(20, 40, 20, 40)), 26);
        Assert.assertEquals(4, bufArray.getCurPacageIndex());
    }

    /**
     * 创建4个报文，第二个报文的type被截断，后面的报文type不被截断，测试是否能读到type
     * @throws IOException
     */
    @Test
    public void testExceeded() throws IOException {
        ByteBufferArray bufArray = splitPackage(createMultiPackage(Arrays.asList(16, 30, 40, 20)), 26);
        Assert.assertEquals(4, bufArray.getCurPacageIndex());
        for (int i = 0; i < 4; i++) {
            // type=0+1+2+3+4+5+6+7
            Assert.assertEquals(28, bufArray.getPacageType(i));
        }
    }

    private ByteArrayInputStream createMultiPackage(List<Integer> lenList) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (int len : lenList) {
            baos.write(createPackage(len));
        }
        baos.flush();
        return new ByteArrayInputStream(baos.toByteArray());
    }
}
