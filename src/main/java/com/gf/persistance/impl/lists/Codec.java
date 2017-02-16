package com.gf.persistance.impl.lists;

public interface Codec<T> {
	T decode(final byte[] bytes);
	byte[] encode(final T object);
}
