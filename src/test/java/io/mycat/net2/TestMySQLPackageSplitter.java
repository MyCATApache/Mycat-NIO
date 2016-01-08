package io.mycat.net2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import junit.framework.Assert;

public class TestMySQLPackageSplitter {

	/** 待测试报文数量 */
	private static final int PACKAGES_COUNT = 10;

	@Test
	public void testSpliter() throws IOException {
		SharedBufferPool sharedPool = new SharedBufferPool(1024 * 1024 * 100, 10240);
		ReactorBufferPool reactBufferPool = new ReactorBufferPool(sharedPool, Thread.currentThread(), 1000);
		ByteBufferArray bufArray = reactBufferPool.allocate();
		bufArray.addNewBuffer();
		int readBufferOffset = 0;
		// @todo 构造符合MYSQL报文的数据包，进行测试

		ByteArrayInputStream mysqlPackgsStream = createMysgqlPackages();
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
		Assert.assertEquals(PACKAGES_COUNT, bufArray.getCurPacageIndex());
	}

	/**
	 * 随机构造报文进行测试，报文长度按照报文协议特点，在0xFF-0xFFFF-0xFFFFFF上均匀分布
	 * @return
	 */
	private ByteArrayInputStream createMysgqlPackages() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			for (int i = 0; i < PACKAGES_COUNT; i++) {
				switch ((int) (Math.random() * 3)) {
				case 0:
					baos.write(createPackage((int) (Math.random() * 0xFF)));
					break;
				case 1:
					baos.write(createPackage((int) (Math.random() * 0xFFFF)));
					break;
				case 2:
					baos.write(createPackage((int) (Math.random() * 0xFFFFFF)));
					break;
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new ByteArrayInputStream(baos.toByteArray());
	}

	/**
	 * 根据MySQL报文格式构造内容长度为len的报文（总长度应为len+4）
	 * @param len 内容长度
	 * @return 报文字节数组
	 */
	private byte[] createPackage(int len) {
		byte[] p = new byte[len + 4];
		p[0] = (byte) (len & 0xFF);
		p[1] = (byte) ((len >> 8) & 0xFF);
		p[2] = (byte) ((len >> 16) & 0xFF);
		return p;
	}
}
