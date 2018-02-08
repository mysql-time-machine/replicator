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
 * Copied from: https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/StringColumn.java
 *              and renamed StringColum to StringCell
 *
 */
public final class StringCell implements Cell {
    //
    private static final long serialVersionUID = 1009717372407166422L;

    //
    private final byte[] value;

    /**
     *
     */
    private StringCell(byte[] value) {
        this.value = value;
    }

    /**
     *
     */
    @Override
    public String toString() {
        return new String(this.value);
    }

    /**
     *
     */
    public byte[] getValue() {
        return this.value;
    }

    /**
     *
     */
    public static final StringCell valueOf(byte[] value) {
        return new StringCell(value);
    }
}