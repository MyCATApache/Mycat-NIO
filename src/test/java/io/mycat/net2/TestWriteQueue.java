package io.mycat.net2;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;

import junit.framework.Assert;

/**
 * 测试WriteQueue功能
 * @author wuzhih
 *
 */
public class TestWriteQueue {

	@Test
	public void testInsertAndGet()
	{
		String testS="test data only for ";
		SharedBufferPool sharedPool = new SharedBufferPool(1024 * 1024 * 100, testS.length()+4);
		ReactorBufferPool reactBufferPool = new ReactorBufferPool(sharedPool, Thread.currentThread(), 1000);
		WriteQueue queue=new WriteQueue(1024*1024*20);
		for(int i=0;i<10000;i++)
		{ByteBufferArray bufArray = reactBufferPool.allocate();
			ByteBuffer buf = bufArray.addNewBuffer();
			buf.put(new String(testS + i).getBytes());
			// 将这次分配好的bufferArray放入写队列，不应重新分配
			queue.add(bufArray);
		}
		for(int i=0;i<10000;i++)
		{
			ByteBufferArray arry=queue.pull();
			ByteBuffer byteBuff=arry.getLastByteBuffer();
			byteBuff.flip();
			byte[] data=new byte[byteBuff.remaining()];
			// 从ByteBuffer中正确读入数据
			byteBuff.get(data);
			// 字节数组使用Arrays.equals比较
			Assert.assertTrue(Arrays.equals(data, (testS+i).getBytes()));
		}
	}
}
