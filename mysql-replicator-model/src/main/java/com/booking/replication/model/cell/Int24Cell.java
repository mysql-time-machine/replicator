package com.booking.replication.model.cell;

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
 * taken from: https://raw.githubusercontent.com/whitesock/open-replicator/master/src/main/java/com/google/code/or/common/glossary/column/Int24Column.java
 */
public final class Int24Cell implements Cell {
    //
    private static final long serialVersionUID = 6456161237369680803L;

    //
    public static final int MIN_VALUE = -8388608;
    public static final int MAX_VALUE =  8388607;

    //
    private static final Int24Cell[] CACHE = new Int24Cell[255];
    static {
        for(int i = 0; i < CACHE.length; i++) {
            CACHE[i] = new Int24Cell(i + Byte.MIN_VALUE);
        }
    }

    //
    private final int value;

    /**
     *
     */
    private Int24Cell(int value) {
        this.value = value;
    }

    /**
     *
     */
    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    /**
     *
     */
    public Integer getValue() {
        return this.value;
    }

    /**
     *
     */
    public static final Int24Cell valueOf(int value) {
        if(value < MIN_VALUE || value > MAX_VALUE) throw new IllegalArgumentException("invalid value: " + value);
        final int index = value - Byte.MIN_VALUE;
        return (index >= 0 && index < CACHE.length) ? CACHE[index] : new Int24Cell(value);
    }
}
