package io.mycat.net2;

import java.io.IOException;

public class NIOReactorPool {
	private final NIOReactor[] reactors;
	private volatile int nextReactor;

	public NIOReactorPool(String name, int poolSize, SharedBufferPool shearedBufferPool) throws IOException {
		reactors = new NIOReactor[poolSize];
		for (int i = 0; i < poolSize; i++) {
			NIOReactor reactor = new NIOReactor(name + "-" + i, shearedBufferPool);
			reactors[i] = reactor;
			reactor.startup();
		}
	}

	public NIOReactor getNextReactor() {
		int i = ++nextReactor;
		if (i >= reactors.length) {
			i = nextReactor = 0;
		}
		return reactors[i];
	}
}
