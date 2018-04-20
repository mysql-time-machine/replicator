package com.booking.replication.augmenter.cell;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Jingqi Xu
 */

/**
 *
 * @author Jingqi Xu
 */

/**
 * taken from: https://raw.githubusercontent.com/whitesock/open-replicator/master/src/main/java/com/google/code/or/common/glossary/column/LongColumn.java
 * and renamed Column to Cell
 */
public final class LongCell implements Cell {
    //
    private static final long serialVersionUID = -4109941053716659749L;

    //
    public static final int MIN_VALUE = Integer.MIN_VALUE;
    public static final int MAX_VALUE = Integer.MAX_VALUE;

    //
    private static final LongCell[] CACHE = new LongCell[255];

    static {
        for (int i = 0; i < CACHE.length; i++) {
            CACHE[i] = new LongCell(i + Byte.MIN_VALUE);
        }
    }

    //
    private final int value;

    /**
     *
     */
    private LongCell(int value) {
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
    public static final LongCell valueOf(int value) {
        if (value < MIN_VALUE || value > MAX_VALUE) throw new IllegalArgumentException("invalid value: " + value);
        final int index = value - Byte.MIN_VALUE;
        return (index >= 0 && index < CACHE.length) ? CACHE[index] : new LongCell(value);
    }
}