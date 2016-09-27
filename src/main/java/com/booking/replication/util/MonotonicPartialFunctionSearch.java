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
     * A pair of int and value comparable to a value.
     *
     * @param <V> value type
     */
    private static class ValueAtPosition<V extends Comparable<V>> implements Comparable<V>{
        private final int position;
        private final V value;

        private ValueAtPosition(int position, V value) {
            this.position = position;
            this.value = value;
        }

        public int getPosition() {
            return position;
        }

        @Override
        public int compareTo(V v) {
            return value.compareTo(v);
        }
    }

    private final Function<T,V> function;

    private final T[] domain;
    private final V[] values;
    private final boolean[] map;

    public MonotonicPartialFunctionSearch(Function<T, V> function, T[] domain) {
        this.function = function;
        this.domain = domain;
        values = (V[]) new Object[domain.length];
        map = new boolean[domain.length];
    }

    /**
     * Returns the greatest lower bound of the pre-image of the given value. I.e. the largest element
     * of the domain such that its image is less then or equal to the value
     *
     * @param value the given value
     * @return the largest element of the domain having the image less then or equal to the value
     */
    public T reverseGLB(V value){

        int l = 0;
        int h = domain.length - 1;

        int cmp;

        ValueAtPosition hf = getClosestDefined(h,l-1,h+1);

        if (hf == null) return null;                                            // the function is not defined at all :(
        if ( hf.compareTo( value ) < 0 ) return domain[h];               // the largest defined value is less then given

        h = hf.getPosition();

        ValueAtPosition lf = getClosestDefined(l,l-1,h+1);
        if (lf == null) return null;                                  // the only defined value is higher than the given

        cmp = lf.compareTo( value );

        if ( cmp > 0 ) {
            return null;                                             // the smallest defined value is greater then given
        } else if ( cmp == 0 ){
            return domain[l];                                           // the smallest defined value equal to the given
        }

        l = lf.getPosition();

        // we maintain invariant f(l) < v < f(h) and we need i such that f(i) <= v < f(i+1)
        while ( h - l > 1){
            int m = l + ( h - l) / 2;                                                                       // l < m < h

            ValueAtPosition mf = getClosestDefined(m,l,h);

            if ( mf == null ) return domain[l];                               // function is not defined between l and h

            cmp = mf.compareTo( value );
            m = mf.getPosition();

            if (cmp == 0) {

                return domain[m];

            } else if (cmp < 0){

                l = m; // maintain v > f(l)

            } else {

                h = m; // maintain v < f(h)

            }

        }

        // h = l + 1, f(l) < v < f(h)

        return domain[l];


    }

    /**
     * Searches for the position between {@code low} and {@code high} exclusive where the function is defined in order:
     * {@code start}, {@code start + 1}, {@code start - 1}, {@code start + 2}, ...
     *
     * @return a defined value at some position of null
     */
    private ValueAtPosition<V> getClosestDefined(int start, int low, int high){

        V value = null;

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

            value = getByIndexCached( position );

            if (value != null ) return new ValueAtPosition<>(position,value);

            positions[selector] += selector == 0 ? -1 : +1; //

        }

    }

    private V getByIndexCached(int i){

        if ( !map[i] ) values[i] = function.apply( domain[i] );

        return values[i];
    }

}
