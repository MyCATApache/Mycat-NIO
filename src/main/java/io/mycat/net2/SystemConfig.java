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
package io.mycat.net2;

import java.io.File;
import java.io.IOException;

/**
 * 系统基础配置项
 * 
 * @author mycat
 */
public final class SystemConfig {
	public static  String SYS_HOME = "MYCAT_HOME";
	public static final int DEFAULT_PROCESSORS = Runtime.getRuntime().availableProcessors();
	private int frontSocketSoRcvbuf = 1024 * 1024;
	private int frontSocketSoSndbuf = 4 * 1024 * 1024;
	private int backSocketSoRcvbuf = 4 * 1024 * 1024;// mysql 5.6
														// net_buffer_length
														// defaut 4M
	private int backSocketSoSndbuf = 1024 * 1024;
	private int frontSocketNoDelay = 1; // 0=false
	private int backSocketNoDelay = 1; // 1=true

	private long processorBufferPool;
	private int processorBufferChunk;

	private int packetHeaderSize = 4;
	private int maxPacketSize = 16 * 1024 * 1024;

	public SystemConfig() {

	}

	public int getPacketHeaderSize() {
		return packetHeaderSize;
	}

	public void setPacketHeaderSize(int packetHeaderSize) {
		this.packetHeaderSize = packetHeaderSize;
	}

	public int getMaxPacketSize() {
		return maxPacketSize;
	}

	public void setMaxPacketSize(int maxPacketSize) {
		this.maxPacketSize = maxPacketSize;
	}

	public static String getHomePath() {
		String home = System.getProperty(SystemConfig.SYS_HOME);
		if (home != null) {
			if (home.endsWith(File.pathSeparator)) {
				home = home.substring(0, home.length() - 1);
				System.setProperty(SystemConfig.SYS_HOME, home);
			}
		}

		// MYCAT_HOME为空，默认尝试设置为当前目录或上级目录。BEN
		if (home == null) {
			try {
				String path = new File("..").getCanonicalPath().replaceAll("\\\\", "/");
				File conf = new File(path + "/conf");
				if (conf.exists() && conf.isDirectory()) {
					home = path;
				} else {
					path = new File(".").getCanonicalPath().replaceAll("\\\\", "/");
					conf = new File(path + "/conf");
					if (conf.exists() && conf.isDirectory()) {
						home = path;
					}
				}

				if (home != null) {
					System.setProperty(SystemConfig.SYS_HOME, home);
				}
			} catch (IOException e) {
				// 如出错，则忽略。
			}
		}

		return home;
	}

	public int getFrontsocketsorcvbuf() {
		return frontSocketSoRcvbuf;
	}

	public int getFrontsocketsosndbuf() {
		return frontSocketSoSndbuf;
	}

	public int getBacksocketsorcvbuf() {
		return backSocketSoRcvbuf;
	}

	public int getBacksocketsosndbuf() {
		return backSocketSoSndbuf;
	}

	public long getProcessorBufferPool() {
		return processorBufferPool;
	}

	public void setProcessorBufferPool(long processorBufferPool) {
		this.processorBufferPool = processorBufferPool;
	}

	public int getProcessorBufferChunk() {
		return processorBufferChunk;
	}

	public void setProcessorBufferChunk(int processorBufferChunk) {
		this.processorBufferChunk = processorBufferChunk;
	}

	public int getFrontSocketSoRcvbuf() {
		return frontSocketSoRcvbuf;
	}

	public void setFrontSocketSoRcvbuf(int frontSocketSoRcvbuf) {
		this.frontSocketSoRcvbuf = frontSocketSoRcvbuf;
	}

	public int getFrontSocketSoSndbuf() {
		return frontSocketSoSndbuf;
	}

	public void setFrontSocketSoSndbuf(int frontSocketSoSndbuf) {
		this.frontSocketSoSndbuf = frontSocketSoSndbuf;
	}

	public int getBackSocketSoRcvbuf() {
		return backSocketSoRcvbuf;
	}

	public void setBackSocketSoRcvbuf(int backSocketSoRcvbuf) {
		this.backSocketSoRcvbuf = backSocketSoRcvbuf;
	}

	public int getBackSocketSoSndbuf() {
		return backSocketSoSndbuf;
	}

	public void setBackSocketSoSndbuf(int backSocketSoSndbuf) {
		this.backSocketSoSndbuf = backSocketSoSndbuf;
	}

	public int getFrontSocketNoDelay() {
		return frontSocketNoDelay;
	}

	public void setFrontSocketNoDelay(int frontSocketNoDelay) {
		this.frontSocketNoDelay = frontSocketNoDelay;
	}

	public int getBackSocketNoDelay() {
		return backSocketNoDelay;
	}

	public void setBackSocketNoDelay(int backSocketNoDelay) {
		this.backSocketNoDelay = backSocketNoDelay;
	}

}