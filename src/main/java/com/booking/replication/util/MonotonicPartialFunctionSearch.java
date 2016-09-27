package com.booking.replication.util;

import java.util.function.Function;

/**
 *
 * An utility class for working with a monotonically increasing partially defined on some domain function.
 *
 */
public class MonotonicPartialFunctionSearch<T,V extends Comparable<V>> {

    /**
     * Returns the largest element of the given domain such that its image under the given function is less then or
     * equal to the given value.
     *
     * @param function a monotonically increasing function partially defined on the domain
     * @param domain the domain sorted in the increasing order
     * @param value the value
     * @param <T> a type of the domain elements
     * @param <V> a type of the value
     * @return the largest element of the domains having the image less then or equal to the value or null if there is
     * no such element in the domain
     */
    public static <T,V extends Comparable<V>> T reverseGLB(Function<? extends T,? extends V> function, T[] domain, V value){
        MonotonicPartialFunctionSearch<T,V> search = new MonotonicPartialFunctionSearch(function, domain);
        return search.reverseGLB(value);
    }

    /**
     * Encapsulates a non-null value, a position this value was found at, and the interval that was checked to find this
     * position. It is comparable to a value.
     *
     * @param <V> value type
     */
    private static class ScanForValueResult<V extends Comparable<V>> implements Comparable<V>{
        private final int position;
        private final V value;
        private final int[] range;

        private ScanForValueResult(int position, V value, int[] range) {
            this.position = position;
            this.value = value;
            this.range = range;
        }

        public int getPosition() {
            return position;
        }

        public int[] getRange() {
            return range;
        }

        @Override
        public int compareTo(V v) {
            return value.compareTo(v);
        }
    }

    private final Function<T,V> function;
    private final T[] domain;

    public MonotonicPartialFunctionSearch(Function<T, V> function, T[] domain) {
        this.function = function;
        this.domain = domain;
    }

    /**
     * Returns the greatest lower bound of the pre-image of the given value. I.e. the largest element
     * of the domain such that its image is less then or equal to the value
     *
     * @param value the given value
     * @return the largest element of the domain having the image less then or equal to the value
     */
    public T reverseGLB(V value){

        int[] i = new int[]{ 0, domain.length - 1 };

        int cmp;

        ScanForValueResult hf = scanForValue(i[1], i[0]-1, i[1]+1);

        if (hf == null) return null;                                            // the function is not defined at all :(
        if ( hf.compareTo( value ) < 0 ) return domain[i[1]];               // the largest defined value is less then given

        i[1] = hf.getPosition();

        ScanForValueResult lf = scanForValue(i[0],i[0]-1,i[1]+1);
        if (lf == null) return null;                                  // the only defined value is higher than the given

        cmp = lf.compareTo( value );

        if ( cmp > 0 ) {
            return null;                                             // the smallest defined value is greater then given
        } else if ( cmp == 0 ){
            return domain[i[0]];                                           // the smallest defined value equal to the given
        }

        i[0] = lf.getPosition();

        // we maintain invariant f(l) < v < f(h) and we need i such that f(i) <= v < f(i+1)
        while ( i[1] - i[0] > 1){

            ScanForValueResult mf = scanForValue( i[0] + ( i[1] - i[0]) / 2, i[0], i[1]);     // search around a point m where l < m < h

            if ( mf == null ) return domain[i[0]];                               // function is not defined between l and h

            cmp = Integer.signum( mf.compareTo( value ) );
            int m = mf.getPosition();

            if (cmp == 0) {

                return domain[m];

            } else {

                // l < scannedLow < scannedHigh < h where m is either scannedLow or scannedHigh

                int direction = cmp < 0 ? 1 : 0;   // 1 means we going to search in the higher segment, 0 - in the lower

                if (m == mf.getRange()[direction]){

                    // either m == scannedLow and f(m) > v or m == scannedHigh and f(m) < v
                    // in the both cases we can use m as a boundary of the next interval to consider

                    i[1 - direction] = m;

                } else {

                    int[] ni = new int[]{i[0],i[1]};
                    ni[1-direction] = mf.getRange()[direction];

                    ScanForValueResult mof = scanForValue(mf.getRange()[direction]+cmp,ni[0],ni[1]);

                    if (mof == null) return domain[m];

                    cmp = mof.compareTo(value);
                    if (cmp == 0){
                        return domain[mof.getPosition()];
                    } else if ( ( cmp < 0 && direction == 1) || (cmp > 0 && direction == 0) ){

                        ni[1-direction] = mof.getPosition();

                    } else {

                        return direction == 1 ? domain[m] : domain[mof.getPosition()];

                    }

                }

            }

        }

        // h = l + 1, f(l) < v < f(h)

        return domain[i[0]];


    }

    /**
     * Searches for the position between {@code low} and {@code high} exclusive where the function is defined in order:
     * {@code start}, {@code start + 1}, {@code start - 1}, {@code start + 2}, ...
     *
     * @return a defined value at some position of null
     */
    private ScanForValueResult<V> scanForValue(int start, int low, int high){

        int selector = 0;
        int[] positions = new int[] { start , start + 1 };

        for (;;){

            if ( positions[0] <= low && positions[1] >= high ){

                //  search in the both directions went beyond the limit so no more values to test :(

                return null;

            } else if ( low >= positions[selector] || positions[selector] >= high ){

                // search in the current direction went beyond the limit, so we flip the direction

                selector = 1 - selector;

            }

            int position = positions[selector];

            V value = function.apply( domain[position] );

            if (value != null ) return new ScanForValueResult<>(position, value, positions);

            positions[selector] += selector == 0 ? -1 : +1;

        }

    }

}
