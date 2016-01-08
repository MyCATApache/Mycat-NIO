package io.mycat.net2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import junit.framework.Assert;

public class TestMySQLPackageSplitter {

	@Test
	public void testSpliter() throws IOException {
		String testS = "test data only for ";
		SharedBufferPool sharedPool = new SharedBufferPool(1024 * 1024 * 100, testS.length() + 4);
		ReactorBufferPool reactBufferPool = new ReactorBufferPool(sharedPool, Thread.currentThread(), 1000);
		ByteBufferArray bufArray = reactBufferPool.allocate();
		bufArray.addNewBuffer();
		int readBufferOffset = 0;
		// @todo 构造符合MYSQL报文的数据包，进行测试

		ByteArrayInputStream mysqlPackgsStream = createMysgqlPackages();
		while (mysqlPackgsStream.available() > 0) {
			ByteBuffer curBuf = bufArray.getLastByteBuffer();
			if(!curBuf.hasRemaining())
			{
				curBuf=bufArray.addNewBuffer();
			}
			byte[] data = new byte[curBuf.remaining()];
			int readed = mysqlPackgsStream.read(data);
			curBuf.put(data, 0, readed);
			readBufferOffset = CommonPackageUtil.parsePackages(bufArray, curBuf, readBufferOffset);

		}
		Assert.assertEquals(10, bufArray.getCurPacageIndex());
	}

	private ByteArrayInputStream createMysgqlPackages() {
		// TODO Auto-generated method stub
		return null;
	}
}
