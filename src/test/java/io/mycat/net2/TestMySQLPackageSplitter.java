package io.mycat.net2;

import java.nio.ByteBuffer;

import org.junit.Test;

import junit.framework.Assert;

public class TestMySQLPackageSplitter {

	@Test
	public void testSpliter() {
		String testS = "test data only for ";
		SharedBufferPool sharedPool = new SharedBufferPool(1024 * 1024 * 100, testS.length() + 4);
		ReactorBufferPool reactBufferPool = new ReactorBufferPool(sharedPool, Thread.currentThread(), 1000);
		ByteBufferArray bufArray = reactBufferPool.allocate();
		//@todo 构造符合MYSQL报文的数据包，进行测试
		for (int j = 0; j < 10; j++) {
			ByteBuffer buf = bufArray.addNewBuffer();
			buf.put(new String(testS + j).getBytes());
		}
		int blockCount = bufArray.getBlockCount();
		int readBufferOffset = 0;
		for (int i = 0; i < blockCount; i++) {
			readBufferOffset = CommonPackageUtil.parsePackages(bufArray, bufArray.getBlock(i), readBufferOffset);
		}

		Assert.assertEquals(10, bufArray.getCurPacageIndex());
	}
}
