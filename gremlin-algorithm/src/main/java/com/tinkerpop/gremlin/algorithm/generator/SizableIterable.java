package com.tinkerpop.gremlin.algorithm.generator;

import java.util.Collection;
import java.util.Iterator;

/**
 * Utility class to make size determinations for {@link Iterable} for efficient.
 * <p>
 * If the number of elements in an Iterable is known, then the Iterable can be wrapped in this class to make
 * determining the size a constant time operation rather than an iteration over the whole Iterable.
 * <p>
 * Some generators need to know the number of vertices prior to generation. Using this class can speed up the generator.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SizableIterable<T> implements Iterable<T> {

    private final Iterable<T> iterable;
    private final int size;

    /**
     * Wraps the given Iterable with the given size.
     * <p>
     * NOTE: the size MUST be the size of the Iterable. This is not verified.
     */
    public SizableIterable(final Iterable<T> iterable, final int size) {
        if (iterable == null)
            throw new NullPointerException();
        if (size < 0)
            throw new IllegalArgumentException("Size must be positive");
        this.iterable = iterable;
        this.size = size;
    }

    /**
     * Returns the size of the Iterable
     *
     * @return Size of the Iterable
     */
    public int size() {
        return size;
    }

    @Override
    public Iterator<T> iterator() {
        return iterable.iterator();
    }

    /**
     * Utility method to determine the number of elements in an Iterable.
     * <p>
     * Checks if the given Iterable is an instance of a Collection or SizableIterable before iterating to improve performance.
     *
     * @return The size of the given Iterable.
     */
    public static <T> int sizeOf(final Iterable<T> iterable) {
        if (iterable instanceof Collection) {
            return ((Collection<T>) iterable).size();
        } else if (iterable instanceof SizableIterable) {
            return ((SizableIterable<T>) iterable).size();
        } else {
            int size = 0;
            for (Iterator<T> iter = iterable.iterator(); iter.hasNext(); iter.next()) {
                size++;
            }
            return size;
        }
    }

}
