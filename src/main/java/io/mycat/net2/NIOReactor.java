package io.mycat.net2;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 网络事件反应器
 * 
 * @author wuzh
 */
public final class NIOReactor {
	private static final Logger LOGGER = LoggerFactory.getLogger(NIOReactor.class);
	private final String name;
	private final RWThread reactorR;
	private final SharedBufferPool shearedBufferPool;

	public NIOReactor(String name, SharedBufferPool shearedBufferPool) throws IOException {
		this.name = name;
		this.shearedBufferPool = shearedBufferPool;
		this.reactorR = new RWThread();
	}

	final void startup() {
		new Thread(reactorR, name + "-RW").start();
	}

	final void postRegister(Connection c) {
		reactorR.registerQueue.offer(c);
		reactorR.selector.wakeup();
	}

	final Queue<Connection> getRegisterQueue() {
		return reactorR.registerQueue;
	}

	final long getReactCount() {
		return reactorR.reactCount;
	}

	private final class RWThread extends Thread {
		private final Selector selector;
		private final ConcurrentLinkedQueue<Connection> registerQueue;
		private long reactCount;
		private final ReactorBufferPool myBufferPool;

		private RWThread() throws IOException {
			this.selector = Selector.open();
			myBufferPool = new ReactorBufferPool(shearedBufferPool, this, 1000);
			this.registerQueue = new ConcurrentLinkedQueue<Connection>();
		}

		@Override
		public void run() {
			final Selector selector = this.selector;
			Set<SelectionKey> keys = null;
			for (;;) {
				++reactCount;
				try {
					selector.select(500L);
					register(selector);
					keys = selector.selectedKeys();
					for (SelectionKey key : keys) {
						Connection con = null;
						try {
							Object att = key.attachment();
							if (att != null && key.isValid()) {
								con = (Connection) att;
								if (key.isReadable()) {
									try {
										con.asynRead();
									} catch (Throwable e) {
										if (!(e instanceof java.io.IOException)) {
											LOGGER.warn("caught err: " + con, e);
										}
										con.close("program err:" + e.toString());
										continue;
									}
								}
								if (key.isWritable()) {
									con.doWriteQueue();
								}
							} else {
								key.cancel();
							}
						} catch (Throwable e) {
							if (e instanceof CancelledKeyException) {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug(con + " socket key canceled");
								}
							} else {
								LOGGER.warn(con + " " + e);
							}

						}

					}
				} catch (Throwable e) {
					LOGGER.warn(name, e);
				} finally {
					if (keys != null) {
						keys.clear();
					}
				}
			}
		}

		private void register(Selector selector) {

			if (registerQueue.isEmpty()) {
				return;
			}
			Connection c = null;
			while ((c = registerQueue.poll()) != null) {
				try {
					c.register(selector, myBufferPool);
				} catch (Throwable e) {
					LOGGER.warn("register error ", e);
					c.close("register err");
				}
			}
		}

	}

}