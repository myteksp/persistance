package com.gf.persistance;

import java.util.Collection;
import java.util.List;

public interface PersistedLongList<T> extends PersistedCollection, Iterable<T>{
	long size();
	boolean isEmpty();
	boolean contains(final Object o);
	Object[] toArray();
	T[] toArray(T[] a);
	boolean add(final T e);
	boolean remove(final Object o);
	boolean containsAll(final Collection<?> c);
	boolean addAll(final Collection<? extends T> c);
	boolean addAll(final long index, final Collection<? extends T> c);
	boolean removeAll(final Collection<?> c);
	boolean retainAll(final Collection<?> c);
	void clear();
	T get(final long index);
	T set(final long index, final T element);
	void add(final long index, final T element);
	T remove(final long index);
	long indexOf(final Object o);
	long lastIndexOf(final Object o);
	LongListIterator<T> listIterator();
	LongListIterator<T> listIterator(final long index);
	List<T> subList(final long fromIndex, final long toIndex);
}
