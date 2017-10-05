package com.booking.replication.util;

import java.lang.reflect.Array;
import java.util.function.Function;

/**
 *
 * A utility class for working with a monotonically increasing, partially defined function (that is, it's not defined
 * for all elements in the domain).
 *
 */
public class MonotonicPartialFunctionSearch<V extends Comparable<V>> {

    private enum ComparisonResult {
        LESS, EQUAL, GREATER, UNKNOWN, UNDEFINED;

        private static ComparisonResult[] map = new ComparisonResult[]{LESS, EQUAL, GREATER};

        public static ComparisonResult valueOf(int i){
            return map[ Integer.signum(i) + 1 ];
        }

        public static ComparisonResult valueOf( Comparable v, Comparable w ){

            if (v == null){
                return UNDEFINED;
            } else {
                return valueOf(v.compareTo(w));
            }

        }
    }

    /**
     * Represents an integer interval together with the results of comparing the given value to the value of the
     * function on its ends
     */
    private class Interval {

        private final int low;
        private final int high;
        private final ComparisonResult lowCmp;
        private final ComparisonResult highCmp;

        public Interval(int low, int high){
            this(low, high, ComparisonResult.UNKNOWN, ComparisonResult.UNKNOWN);
        }

        public Interval(int low, int high, ComparisonResult lowCmp, ComparisonResult highCmp){
            this.low = low;
            this.high = high;
            this.lowCmp = lowCmp;
            this.highCmp = highCmp;
        }

        public int getLow() { return low; }

        public int getHigh() { return high; }

        public int length() { return getHigh() - getLow() + 1; }

        public ComparisonResult getLowCmp() { return lowCmp; }

        public ComparisonResult getHighCmp() { return highCmp; }

        /**
         * Returns the maximal subinterval of {@code this} such that the function is defined on its both ends.
         *
         * @param v the value to compare with
         * @return the interval or null
         */
        public Interval shrink(V v){

            Interval i = shrinkHigh(v);

            if (i != null) i = i.shrinkLow(v);

            return i;
        }

        /**
         * Returns the maximal subinterval of {@code this} such that the function is defined on its high end.
         *
         * @param v the value to compare with
         * @return the interval or null
         */
        public Interval shrinkHigh(V v){ return shrink(v, 1); }

        /**
         * Returns the maximal subinterval of {@code this} such that the function is defined on its low end.
         *
         * @param v the value to compare with
         * @return the interval or null
         */
        public Interval shrinkLow(V v){ return shrink(v, 0); }

        private Interval shrink(V v, int selector){

            int[] limits = new int[]{ getLow(), getHigh() };

            ComparisonResult[] cmps = new ComparisonResult[]{ getLowCmp(), getHighCmp() };

            while (shrinkStep( limits, cmps, v, selector ));

            if (cmps[selector] != ComparisonResult.UNDEFINED ) {
                return new Interval( limits[0], limits[1], cmps[0], cmps[1] );
            } else {
                return null;
            }

        }

        private boolean shrinkStep(int[] limits, ComparisonResult[] cmps, V v, int selector){

            if ( cmps[selector] != ComparisonResult.UNKNOWN && ( limits[0] == limits[1] || cmps[selector] != ComparisonResult.UNDEFINED) ) return false;

            if (cmps[selector] == ComparisonResult.UNDEFINED && limits[0] < limits[1]){

                limits[selector] += selector == 0 ? +1 : -1;

                cmps[selector] = limits[0] == limits[1] ? cmps[1-selector] : ComparisonResult.UNKNOWN;

            }

            if (cmps[selector] == ComparisonResult.UNKNOWN){

                cmps[selector] = ComparisonResult.valueOf( f.apply( limits[selector] ), v );

                if (limits[0] == limits[1]) cmps[1-selector] = cmps[selector];

            }

            return true;
        }

        /**
         * Splits this interval {@code [low,high]} around the middle onto {@code [low,a],[b,high] } in such a way that the
         * function is defined either at {@code a} or at {@code b} and is not defined anywhere between.
         *
         * Assumes the {@code high - low > 1} and the function is defined at the both {@code low,high}
         *
         * @param v the value to compare with
         * @return two intervals {@code [low,a],[b,high] }
         */
        public Interval[] split(V v){

            assert getHighCmp() != ComparisonResult.UNKNOWN && getHighCmp() != ComparisonResult.UNDEFINED;
            assert getLowCmp() != ComparisonResult.UNKNOWN && getLowCmp() != ComparisonResult.UNDEFINED;
            assert getHigh() - getLow() > 1;

            int low = getLow();
            int high = getHigh();
            int middle = low + ( high - low ) / 2;

            int[] lower = new int[] { low, middle };
            int[] higher = new int[] { middle, high };

            ComparisonResult[] lowerCmp = new ComparisonResult[]{ getLowCmp(), ComparisonResult.UNKNOWN };
            ComparisonResult[] higherCmp = new ComparisonResult[]{ ComparisonResult.UNKNOWN, getHighCmp() };

            lowerCmp[1] = higherCmp[0] = ComparisonResult.valueOf( f.apply( middle ), v );

            while ( shrinkStep( lower, lowerCmp, v, 1 ) && shrinkStep( higher, higherCmp, v, 0 ) );

            Interval[] result = (Interval[]) Array.newInstance(Interval.class,2);
            result[0] = new Interval(lower[0], lower[1], lowerCmp[0], lowerCmp[1]);
            result[1] = new Interval(higher[0], higher[1], higherCmp[0], higherCmp[1]);

            return result;
        }

    }

    /**
     * Returns the largest element of the given interval such that its image under the given function is less than or
     * equal to the given value.
     *
     * @param f a monotonically increasing function partially defined on the interval
     * @param value the value
     * @param low lower bound
     * @param high upper bound
     * @param <V> a type of the value
     * @return the largest integer {@code i} such that {@code low <= i <= high} and {@code f(i) <= value}, or null
     */
    public static <V extends Comparable<V>> Integer preimageGLB( Function<Integer, V> f, V value, int low, int high ){

        MonotonicPartialFunctionSearch<V> monotonicPartialFunctionSearch = new MonotonicPartialFunctionSearch<>(f);
        return monotonicPartialFunctionSearch.preimageGLB( value, low, high );

    }

    /**
     * Returns the largest element of the given domain such that its image under the given function is less than or
     * equal to the given value.
     *
     * @param f a monotonically increasing function partially defined on the domain
     * @param domain the array of points to examine the function on. The function, where defined, should be monotonically
     *               increasing regarding the order of elements in the array
     * @param value the value
     * @param <T> a type of the domain elements
     * @param <V> a type of the value
     * @return the element {@code domain[i]} such that {@code f(domain[i]) <= value} and either
     * {@code i+1 > domain.length} or {@code f(domain[i+1]) > value}, or null
     */
    public static <T,V extends Comparable<V>> T preimageGLB( Function<T, V> f, V value, T[] domain ){

        Integer i = preimageGLB( x->f.apply(domain[x]), value, 0, domain.length-1 );

        if (i == null) return null;

        return domain[i];
    }

    private Function<Integer, V> f;

    public MonotonicPartialFunctionSearch(Function<Integer, V> f){
        this.f = f;
    }

    /**
     * Returns the greatest lower bound of the pre-image of the given value. I.e. the largest element
     * of the interval such that its image is less than or equal to the value
     *
     * @param v the given value
     * @param low minimum
     * @param high maximum
     * @return the largest element of the domain having the image less than or equal to the value
     */
    public Integer preimageGLB(V v, int low, int high){

        if ( high < low ) return null;

        Interval current = new Interval( low, high).shrinkHigh(v); // linear search from the high end till we find the highest point where the function is defined

        if ( current == null ) return null; // the function is not defined on [low,high]

        if ( current.getHighCmp() == ComparisonResult.LESS || current.getHighCmp() == ComparisonResult.EQUAL ) return current.getHigh();

        int step = 1;

        Interval next;

        int next_low;

        // we decrease c.l to low unless f(c.l) <= v maintaining f(c.h) > v
        do {

            if ( current.getHigh() == low ) return null; // f(c.h) > v hence f(l) > v

            next_low = current.getHigh() - step < low ? low : current.getHigh() - step;

            next = new Interval( next_low, current.getHigh(), ComparisonResult.UNKNOWN, ComparisonResult.GREATER ).shrinkLow(v);

            if (next.getLowCmp() != ComparisonResult.GREATER ){

                current = next;

                break;

            } else {

                current = new Interval(next_low,next_low,ComparisonResult.GREATER,ComparisonResult.GREATER );

            }

            step *= 2;

        } while (next_low != low);

        if ( current.getLowCmp() == ComparisonResult.EQUAL ) return current.getLow();

        if ( current.getLowCmp() == ComparisonResult.GREATER ) return null; // lowest defined value on the interval is greater then the given

        // here we have an interval [l,h] such that f(l),f(h) are defined and f(l)<v<f(h)

        while ( current.length() > 2 ){

            Interval[] subintervals = current.split( v );

            Interval lower = subintervals[0];
            Interval higher = subintervals[1];

            if ( lower.getHighCmp() == ComparisonResult.LESS ){
                higher = higher.shrink( v );
            }

            if ( higher.getLowCmp() == ComparisonResult.GREATER ){
                lower = lower.shrink( v );
            }

            if ( lower.getHighCmp() == ComparisonResult.EQUAL ) {

                return lower.getHigh();

            } else if ( higher.getLowCmp() == ComparisonResult.EQUAL ) {

                return higher.getLow();

            } else if ( lower.getHighCmp() == ComparisonResult.GREATER ){

                current = lower;

            } else if ( higher.getLowCmp() == ComparisonResult.LESS ){

                current = higher;

            } else {
                // if non of the previous was true, we are in a situation low <= a < b <= high where f(a) < v, f(b) > v
                // so we exploit the fact that f(i) is not defined for i in (a,b) as forced by the logic of split

                return lower.getHigh();
            }
        }

        return current.getLow();
    }
}