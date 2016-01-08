package io.mycat.net2;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.junit.Test;

import junit.framework.Assert;

public class TestReactorBufferPool {

	@Test
	public void testLocateAndFree() {
		String testS="test data only for ";
		SharedBufferPool sharedPool = new SharedBufferPool(1024 * 1024 * 100, testS.length()+4);
		ReactorBufferPool reactBufferPool = new ReactorBufferPool(sharedPool, Thread.currentThread(), 1000);
		for (int i = 0; i < 10000; i++) {
			ByteBufferArray bufArray = reactBufferPool.allocate();
			for (int j = 0; j < 100; j++) {
				ByteBuffer buf = bufArray.addNewBuffer();
				buf.put(new String(testS + i).getBytes());
			}
			bufArray.recycle();
		}
		Assert.assertEquals(100, sharedPool.getNewCreated());
		Assert.assertEquals(100, reactBufferPool.getCurByteBuffersCount());
	}

	@Test
	public void testMutilTreadLocateAndFree() {
		String testS="test data only for ";
		SharedBufferPool sharedPool = new SharedBufferPool(1024 * 1024 * 100, testS.length()+4);
		// 十个reactor线程
		ArrayList<Thread> runthreads = new ArrayList<Thread>();
		for (int k = 0; k < 10; k++) {
			Thread thread = new Thread() {
				public void run() {
					ReactorBufferPool reactBufferPool = new ReactorBufferPool(sharedPool, Thread.currentThread(), 1000);
					for (int i = 0; i < 10000; i++) {
						ByteBufferArray bufArray = reactBufferPool.allocate();
						for (int j = 0; j < 100; j++) {
							ByteBuffer buf = bufArray.addNewBuffer();
							buf.put(new String(testS + i).getBytes());
						}
						bufArray.recycle();
					}
					Assert.assertEquals(100, reactBufferPool.getCurByteBuffersCount());
				}
			};
			thread.start();
			runthreads.add(thread);
		}
		boolean allFinished = false;
		while (!allFinished) {
			allFinished = true;
			for (Thread thrd : runthreads) {
				if (thrd.isAlive()) {
					allFinished = false;
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		Assert.assertEquals(true, (sharedPool.getNewCreated()<=1000));

	}
}
