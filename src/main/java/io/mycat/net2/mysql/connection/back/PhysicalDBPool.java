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
package io.mycat.net2.mysql.connection.back;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.net2.mysql.definination.Alarms;

public class PhysicalDBPool {

    protected static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDBPool.class);
    private final String hostName;
    protected PhysicalDatasource[] writeSources;
    protected Map<Integer, PhysicalDatasource[]> readSources;
    protected volatile int activedIndex;
    protected volatile boolean initSuccess;
    protected final ReentrantLock switchLock = new ReentrantLock();
    private final Collection<PhysicalDatasource> allDs;
    private String[] schemas;
    private final Random wnrandom = new Random();

    public PhysicalDBPool(String name, PhysicalDatasource[] writeSources,
            Map<Integer, PhysicalDatasource[]> readSources) {
        this.hostName = name;
        this.writeSources = writeSources;
        Iterator<Map.Entry<Integer, PhysicalDatasource[]>> entryItor = readSources.entrySet().iterator();
        while (entryItor.hasNext()) {
            PhysicalDatasource[] values = entryItor.next().getValue();
            if (values.length == 0) {
                entryItor.remove();
            }
        }
        this.readSources = readSources;
        this.allDs = this.genAllDataSources();
        LOGGER.info("total resouces of dataHost " + this.hostName + " is :" + allDs.size());
        setDataSourceProps();
    }

    private void setDataSourceProps() {
        for (PhysicalDatasource ds : this.allDs) {
            ds.setDbPool(this);
        }
    }

    private Collection<PhysicalDatasource> genAllDataSources() {
        LinkedList<PhysicalDatasource> allSources = new LinkedList<PhysicalDatasource>();
        for (PhysicalDatasource ds : writeSources) {
            if (ds != null) {
                allSources.add(ds);
            }
        }
        for (PhysicalDatasource[] dataSources : this.readSources.values()) {
            for (PhysicalDatasource ds : dataSources) {
                if (ds != null) {
                    allSources.add(ds);
                }
            }
        }
        return allSources;
    }

    public PhysicalDatasource[] getSources() {
        return writeSources;
    }

    public boolean isInitSuccess() {
        return initSuccess;
    }

    private boolean checkIndex(int i) {
        return i >= 0 && i < writeSources.length;
    }

    private int loop(int i) {
        return i < writeSources.length ? i : (i - writeSources.length);
    }

    private boolean initSource(int index, PhysicalDatasource ds) {
        int initSize = ds.getConfig().getMinCon();
        LOGGER.info("init backend myqsl source ,create connections total " + initSize + " for " + ds.getName()
                + " index :" + index);
        // long start=System.currentTimeMillis();
        // long timeOut=start+5000*1000L;

        for (int i = 0; i < initSize; i++) {
            try {

                ds.getConnection(this.schemas[i % schemas.length], true, null);
            } catch (Exception e) {
                LOGGER.warn(getMessage(index, " init connection error."), e);
            }
        }
        LOGGER.info("init source finished");
        return true;
    }

    private String getMessage(int index, String info) {
        return new StringBuilder().append(hostName).append(" index:").append(index).append(info).toString();
    }

    public void init(int index) {
        if (!checkIndex(index)) {
            index = 0;
        }
        int active = -1;
        for (int i = 0; i < writeSources.length; i++) {
            int j = loop(i + index);
            if (initSource(j, writeSources[j])) {
                active = j;
                activedIndex = active;
                initSuccess = true;
                LOGGER.info(getMessage(active, " init success"));
            }
        }
        if (!checkIndex(active)) {
            initSuccess = false;
            StringBuilder s = new StringBuilder();
            s.append(Alarms.DEFAULT).append(hostName).append(" init failure");
            LOGGER.error(s.toString());
        }
    }

    private boolean isAlive(PhysicalDatasource theSource) {
        // TODO mock
        return true;
    }

    public PhysicalDatasource getSource() {
        int index = Math.abs(wnrandom.nextInt()) % writeSources.length;
        PhysicalDatasource result = writeSources[index];
        if (!this.isAlive(result)) {
            // find all live nodes
            ArrayList<Integer> alives = new ArrayList<Integer>(writeSources.length - 1);
            for (int i = 0; i < writeSources.length; i++) {
                if (i != index) {
                    if (this.isAlive(writeSources[i])) {
                        alives.add(i);
                    }
                }
            }
            if (alives.isEmpty()) {
                result = writeSources[0];
            } else {
                // random select one
                index = Math.abs(wnrandom.nextInt()) % alives.size();
                result = writeSources[alives.get(index)];

            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("select write source " + result.getName() + " for dataHost:" + hostName);
        }
        return result;
    }

    public int getActivedIndex() {
        return activedIndex;
    }

    public String[] getSchemas() {
        return schemas;
    }

    public void setSchemas(String[] schemas) {
        this.schemas = schemas;
    }

}