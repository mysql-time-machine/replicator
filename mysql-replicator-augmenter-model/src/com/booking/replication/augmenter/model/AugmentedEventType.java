package com.booking.replication.augmenter.model;

/**
 * Created by bdevetak on 4/20/18.
 */

    @SuppressWarnings("unused")
    public enum AugmentedEventType {

        PSEUDO_GTID(0, PseudoGTIDEventData.class);

        private final int code;
        private final Class<? extends AugmentedEventData> definition;
        private final Class<? extends AugmentedEventData> implementation;

        AugmentedEventType(
            int code,
            Class<? extends AugmentedEventData> definition,
            Class<? extends AugmentedEventData> implementation
        ) {
            this.code = code;
            this.definition = definition;
            this.implementation = implementation;
        }

        AugmentedEventType(int code, Class<? extends AugmentedEventData> definition) {
            this(code, definition, null);
        }

        AugmentedEventType(int code) {
            this(code, AugmentedEventData.class);
        }

        public int getCode() {
            return this.code;
        }
    }

