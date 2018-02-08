package com.booking.replication.binlog.common.cell;

import com.booking.replication.binlog.common.Cell;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 * @author Jingqi Xu
 */

/**
 * taken from: https://raw.githubusercontent.com/whitesock/open-replicator/master/src/main/java/com/google/code/or/common/glossary/column/NullColumn.java
 * and renamed Column to Cell
 */
public final class NullCell implements Cell {
    //
    private static final long serialVersionUID = 3300119160243172731L;

    //
    private static final NullCell[] CACHE = new NullCell[255];
    static {
        for(int i = 0; i < CACHE.length; i++) {
            CACHE[i] = new NullCell(i);
        }
    }

    //
    private final int type;

    /**
     *
     */
    private NullCell(int type) {
        this.type = type;
    }

    /**
     *
     */
    @Override
    public String toString() {
        return "null";
    }

    /**
     *
     */
    public int getType() {
        return type;
    }

    public Object getValue() {
        return null;
    }

    /**
     *
     */
    public static final NullCell valueOf(int type) {
        if(type < 0 || type >= CACHE.length) throw new IllegalArgumentException("invalid type: " + type);
        return CACHE[type];
    }
}