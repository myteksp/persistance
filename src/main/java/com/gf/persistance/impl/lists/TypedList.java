package com.gf.persistance.impl.lists;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.gf.persistance.PersistedList;

public final class TypedList<T> implements PersistedList<T>{

	private final PersistedList<byte[]> engine;
	private final Codec<T> codec;

	public TypedList(final PersistedList<byte[]> engine, final Codec<T> codec){
		if (engine == null)
			throw new NullPointerException("Engine can not be null");
		if (codec == null)
			throw new NullPointerException("Codec can not be null");

		this.engine = engine;
		this.codec = codec;
	}
	
	

	@Override
	public final void close() throws IOException {
		engine.close();
	}
	@Override
	public final File getIndexFile(){
		return engine.getIndexFile();
	}
	@Override
	public File getStorageFile(){
		return engine.getStorageFile();
	}
	@Override
	public final int size() {
		return engine.size();
	}

	@Override
	public final boolean isEmpty() {
		return engine.isEmpty();
	}

	@SuppressWarnings("unchecked")
	@Override
	public final boolean contains(final Object o) {
		return engine.contains(codec.encode((T) o));
	}

	@Override
	public final Iterator<T> iterator() {
		return new Iterator<T>() {
			private final Iterator<byte[]> iter = engine.iterator();
			@Override
			public final boolean hasNext() {
				return iter.hasNext();
			}
			@Override
			public final T next() {
				return codec.decode(iter.next());
			}
		};
	}
	@Override
	public final Object[] toArray() {
		final int len = engine.size();
		final Object[] res = new Object[len];

		for (int i = 0; i < len; i++) 
			res[i] = codec.decode(engine.get(i));

		return res;
	}

	@SuppressWarnings({ "unchecked", "hiding" })
	@Override
	public final <T> T[] toArray(final T[] a) {
		final int len = a.length;
		final byte[][] bytes = engine.toArray(new byte[len][]);

		for (int i = 0; i < len; i++) 
			a[i] = (T) codec.decode(bytes[i]);

		return a;
	}

	@Override
	public final boolean add(final T e) {
		return engine.add(codec.encode(e));
	}

	@SuppressWarnings("unchecked")
	@Override
	public final boolean remove(final Object o) {
		return engine.remove(codec.encode((T) o));
	}

	@SuppressWarnings("unchecked")
	@Override
	public final boolean containsAll(final Collection<?> c) {
		final ArrayList<byte[]> col = new ArrayList<byte[]>(c.size());

		for(final Object o : c)
			col.add(codec.encode((T) o));

		return engine.containsAll(col);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final boolean addAll(final Collection<? extends T> c) {
		final ArrayList<byte[]> col = new ArrayList<byte[]>(c.size());

		for(final Object o : c)
			col.add(codec.encode((T) o));
		
		return engine.addAll(col);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final boolean addAll(final int index, final Collection<? extends T> c) {
		final ArrayList<byte[]> col = new ArrayList<byte[]>(c.size());

		for(final Object o : c)
			col.add(codec.encode((T) o));
		
		return engine.addAll(index, col);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final boolean removeAll(final Collection<?> c) {
		final ArrayList<byte[]> col = new ArrayList<byte[]>(c.size());

		for(final Object o : c)
			col.add(codec.encode((T) o));
		
		return engine.removeAll(col);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final boolean retainAll(final Collection<?> c) {
		final ArrayList<byte[]> col = new ArrayList<byte[]>(c.size());

		for(final Object o : c)
			col.add(codec.encode((T) o));
		
		return engine.retainAll(col);
	}

	@Override
	public final void clear() {
		engine.clear();
	}

	@Override
	public final T get(final int index) {
		return codec.decode(engine.get(index));
	}

	@Override
	public final T set(final int index, final T element) {
		return codec.decode(engine.set(index, codec.encode(element)));
	}

	@Override
	public final void add(final int index, final T element) {
		engine.add(index, codec.encode(element));
	}

	@Override
	public final T remove(final int index) {
		return codec.decode(engine.remove(index));
	}

	@SuppressWarnings("unchecked")
	@Override
	public final int indexOf(final Object o) {
		return engine.indexOf(codec.encode((T) o));
	}

	@SuppressWarnings("unchecked")
	@Override
	public final int lastIndexOf(final Object o) {
		return engine.lastIndexOf(codec.encode((T) o));
	}

	@Override
	public final ListIterator<T> listIterator() {
		return new ListIterator<T>() {
			private final ListIterator<byte[]> iter = engine.listIterator();
			@Override
			public final boolean hasNext() {
				return iter.hasNext();
			}
			@Override
			public final T next() {
				return codec.decode(iter.next());
			}
			@Override
			public final boolean hasPrevious() {
				return iter.hasPrevious();
			}
			@Override
			public final T previous() {
				return codec.decode(iter.previous());
			}
			@Override
			public final int nextIndex() {
				return iter.nextIndex();
			}
			@Override
			public final int previousIndex() {
				return iter.previousIndex();
			}
			@Override
			public final void remove() {
				iter.remove();
			}
			@Override
			public final void set(final T e) {
				iter.set(codec.encode(e));
			}
			@Override
			public final void add(T e) {
				iter.add(codec.encode(e));
			}
		};
	}

	@Override
	public final ListIterator<T> listIterator(final int index) {
		return new ListIterator<T>() {
			private final ListIterator<byte[]> iter = engine.listIterator(index);
			@Override
			public final boolean hasNext() {
				return iter.hasNext();
			}
			@Override
			public final T next() {
				return codec.decode(iter.next());
			}
			@Override
			public final boolean hasPrevious() {
				return iter.hasPrevious();
			}
			@Override
			public final T previous() {
				return codec.decode(iter.previous());
			}
			@Override
			public final int nextIndex() {
				return iter.nextIndex();
			}
			@Override
			public final int previousIndex() {
				return iter.previousIndex();
			}
			@Override
			public final void remove() {
				iter.remove();
			}
			@Override
			public final void set(final T e) {
				iter.set(codec.encode(e));
			}
			@Override
			public final void add(T e) {
				iter.add(codec.encode(e));
			}
		};
	}

	@Override
	public final List<T> subList(final int fromIndex, final int toIndex) {
		final List<byte[]> bytes = engine.subList(fromIndex, toIndex);
		final int len = bytes.size();
		final List<T> res = new ArrayList<T>(len);
		
		for (int i = 0; i < len; i++) 
			res.add(codec.decode(bytes.get(i)));
		
		return res;
	}

	@Override
	public synchronized final void delete() {
		try{this.engine.delete();}catch(final Throwable t){}
	}
}
