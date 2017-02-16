package com.gf.persistance;


public interface LongListIterator<T> {
	void set(final T e);
	void remove();
	long previousIndex();
	T previous();
	long nextIndex();
	T next();
	boolean hasPrevious();
	boolean hasNext();
	void add(final T e);
}
