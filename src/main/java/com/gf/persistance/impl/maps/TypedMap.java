package com.gf.persistance.impl.maps;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.gf.persistance.PersistedMap;
import com.gf.persistance.impl.lists.Codec;

public final class TypedMap<K, V> implements PersistedMap<K, V>{
	private final PersistedMap<byte[], byte[]> engine;
	private final Codec<K> keyCodec;
	private final Codec<V> valueCodec;

	public TypedMap(final PersistedMap<byte[], byte[]> engine, final Codec<K> keyCodec, final Codec<V> valueCodec){
		this.engine = engine;
		this.keyCodec = keyCodec;
		this.valueCodec = valueCodec;
	}
	@Override
	public final File getIndexFile() {
		return engine.getIndexFile();
	}
	@Override
	public final File getStorageFile() {
		return engine.getStorageFile();
	}
	@Override
	public final void delete() {
		engine.delete();
	}
	@Override
	public final void close() throws IOException {
		engine.close();
	}
	@Override
	public final int size() {
		return engine.size();
	}

	@Override
	public final boolean isEmpty() {
		return engine.isEmpty();
	}

	@Override
	public final boolean containsKey(final Object key) {
		return engine.containsKey(key);
	}

	@Override
	public final boolean containsValue(final Object value) {
		return engine.containsValue(value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final V get(final Object key) {
		return valueCodec.decode(engine.get(keyCodec.encode((K) key)));
	}

	@Override
	public final V put(final K key, final V value) {
		return valueCodec.decode(engine.put(keyCodec.encode((K) key), valueCodec.encode(value)));
	}

	@SuppressWarnings("unchecked")
	@Override
	public final V remove(final Object key) {
		return valueCodec.decode(engine.remove(keyCodec.encode((K) key)));
	}

	@Override
	public final void putAll(final Map<? extends K, ? extends V> m) {
		for(final Map.Entry<? extends K, ? extends V> ent : m.entrySet())
			this.put(ent.getKey(), ent.getValue());
	}

	@Override
	public final void clear() {
		engine.clear();
	}

	@Override
	public final Set<K> keySet() {
		return new Set<K>() {
			private final Set<byte[]> set = engine.keySet();
			@Override
			public final int size() {
				return set.size();
			}
			@Override
			public final boolean isEmpty() {
				return set.isEmpty();
			}
			@SuppressWarnings("unchecked")
			@Override
			public final boolean contains(final Object o) {
				return engine.containsKey(keyCodec.encode((K) o));
			}
			@Override
			public final Iterator<K> iterator() {
				return new Iterator<K>() {
					private final Iterator<byte[]> iter = set.iterator();
					@Override
					public final boolean hasNext() {
						return iter.hasNext();
					}
					@Override
					public final K next() {
						return keyCodec.decode(iter.next());
					}
				};
			}
			@Override
			public Object[] toArray() {
				final Iterator<K> iter = iterator();
				final ArrayList<K> res = new ArrayList<K>(size());

				while(iter.hasNext())
					res.add(iter.next());

				return res.toArray();
			}

			@Override
			public final <T> T[] toArray(final T[] a) {
				final Iterator<K> iter = iterator();
				final ArrayList<K> res = new ArrayList<K>(size());

				while(iter.hasNext())
					res.add(iter.next());

				return res.toArray(a);
			}
			@Override
			public final boolean add(final K e) {
				return set.add(keyCodec.encode(e));
			}
			@SuppressWarnings("unchecked")
			@Override
			public final boolean remove(final Object o) {
				return set.remove(keyCodec.encode((K) o));
			}
			@SuppressWarnings("unchecked")
			@Override
			public boolean containsAll(final Collection<?> c) {
				final ArrayList<byte[]> col = new ArrayList<byte[]>(c.size());

				for(final Object o : c)
					col.add(keyCodec.encode((K) o));

				return set.containsAll(col);
			}

			@SuppressWarnings("unchecked")
			@Override
			public final boolean addAll(Collection<? extends K> c) {
				final ArrayList<byte[]> col = new ArrayList<byte[]>(c.size());

				for(final Object o : c)
					col.add(keyCodec.encode((K) o));

				return set.addAll(col);
			}
			@SuppressWarnings("unchecked")
			@Override
			public final boolean retainAll(final Collection<?> c) {
				final ArrayList<byte[]> col = new ArrayList<byte[]>(c.size());

				for(final Object o : c)
					col.add(keyCodec.encode((K) o));

				return set.retainAll(col);
			}
			@SuppressWarnings("unchecked")
			@Override
			public final boolean removeAll(Collection<?> c) {
				final ArrayList<byte[]> col = new ArrayList<byte[]>(c.size());

				for(final Object o : c)
					col.add(keyCodec.encode((K) o));

				return set.removeAll(col);
			}
			@Override
			public final void clear() {
				set.clear();
			}
		};
	}

	@Override
	public final Collection<V> values() {
		return new Collection<V>() {
			private final Set<java.util.Map.Entry<K, V>> set = entrySet();
			@Override
			public final int size() {
				return set.size();
			}
			@Override
			public final boolean isEmpty() {
				return set.isEmpty();
			}
			@Override
			public final boolean contains(final Object o) {
				for(final java.util.Map.Entry<K, V> ent : set)
					if (o.equals(ent.getValue()))
						return true;

				return false;
			}
			@Override
			public final Iterator<V> iterator() {
				return new Iterator<V>() {
					private final Iterator<java.util.Map.Entry<K, V>> iter = set.iterator();
					@Override
					public final boolean hasNext() {
						return iter.hasNext();
					}
					@Override
					public final V next() {
						final java.util.Map.Entry<K, V> ent = iter.next();

						if (ent == null)
							return null;

						return ent.getValue();
					}
				};
			}

			@Override
			public Object[] toArray() {
				final ArrayList<V> res = new ArrayList<V>(set.size());

				for(final java.util.Map.Entry<K, V> ent : set)
					res.add(ent.getValue());

				return res.toArray();
			}

			@SuppressWarnings("unchecked")
			@Override
			public <T> T[] toArray(T[] a) {
				int index = 0, len = a.length;
				for(final java.util.Map.Entry<K, V> ent : set){
					if (index < len)
						a[index] = (T) ent.getValue();
					else
						return a;

					index++;
				}
				return a;
			}
			@Override
			public final boolean remove(final Object o) {
				for(final java.util.Map.Entry<K, V> ent : set)
					if (ent.getValue().equals(o))
						return set.remove(ent);
				
				return false;
			}

			@Override
			public final boolean containsAll(final Collection<?> c) {
				for(final Object o : c)
					if (!contains(o))
						return false;
				
				return true;
			}
			@Override
			public final boolean removeAll(final Collection<?> c) {
				boolean result = false;
				for(final Object o : c){
					if (remove(o)){
						result = true;
					}
				}
				return result;
			}

			@Override
			public boolean retainAll(Collection<?> c) {
				final HashSet<Object> set = new HashSet<Object>();
				set.addAll(c);
				final ArrayList<V> toRemove = new ArrayList<V>();

				for(final V o : this)
					if (!set.contains(o))
						toRemove.add(o);

				return this.removeAll(toRemove);
			}
			@Override
			public final boolean add(final V e) {
				throw new RuntimeException("Adding not supported on value set derived from map.");
			}
			@Override
			public final boolean addAll(final Collection<? extends V> c) {
				throw new RuntimeException("Adding not supported on value set derived from map.");
			}
			@Override
			public final void clear() {
				set.clear();
			}
		};
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return new Set<java.util.Map.Entry<K, V>>() {
			private final Set<java.util.Map.Entry<byte[], byte[]>> set = engine.entrySet();

			private final java.util.Map.Entry<byte[], byte[]> encode(final java.util.Map.Entry<K, V> ent){
				return new Entry<byte[], byte[]>() {
					@Override
					public final byte[] setValue(final byte[] value) {
						return valueCodec.encode(ent.setValue(valueCodec.decode(value)));
					}
					@Override
					public final byte[] getValue() {
						return valueCodec.encode(ent.getValue());
					}

					@Override
					public final byte[] getKey() {
						return keyCodec.encode(ent.getKey());
					}
				};
			}
			private final java.util.Map.Entry<K, V> decode(final java.util.Map.Entry<byte[], byte[]> ent){
				return new Entry<K, V>() {
					@Override
					public final K getKey() {
						return keyCodec.decode(ent.getKey());
					}
					@Override
					public final V getValue() {
						return valueCodec.decode(ent.getValue());
					}
					@Override
					public final V setValue(final V value) {
						return valueCodec.decode(ent.setValue(valueCodec.encode(value)));
					}
				};
			}
			@Override
			public final int size() {
				return set.size();
			}
			@Override
			public final boolean isEmpty() {
				return set.isEmpty();
			}
			@SuppressWarnings({ "unchecked", "unlikely-arg-type" })
			@Override
			public final boolean contains(final Object o) {
				if (o == null)
					return false;

				if (o instanceof java.util.Map.Entry){
					return engine.containsKey(encode((java.util.Map.Entry<K, V>) o));
				}
				return false;
			}
			@Override
			public final Iterator<java.util.Map.Entry<K, V>> iterator() {
				return new Iterator<java.util.Map.Entry<K, V>>() {
					private final Iterator<java.util.Map.Entry<byte[], byte[]>> iter = set.iterator();
					@Override
					public final boolean hasNext() {
						return iter.hasNext();
					}
					@Override
					public final java.util.Map.Entry<K, V> next() {
						return decode(iter.next());
					}
				};
			}
			@Override
			public Object[] toArray() {
				final Iterator<java.util.Map.Entry<K, V>> iter = iterator();
				final ArrayList<java.util.Map.Entry<K, V>> res = new ArrayList<java.util.Map.Entry<K, V>>(size());

				while(iter.hasNext())
					res.add(iter.next());

				return res.toArray();
			}

			@Override
			public final <T> T[] toArray(final T[] a) {
				final Iterator<java.util.Map.Entry<K, V>> iter = iterator();
				final ArrayList<java.util.Map.Entry<K, V>> res = new ArrayList<java.util.Map.Entry<K, V>>(size());

				while(iter.hasNext())
					res.add(iter.next());

				return res.toArray(a);
			}
			@Override
			public final boolean add(final java.util.Map.Entry<K, V> e) {
				return set.add(encode(e));
			}
			@SuppressWarnings("unchecked")
			@Override
			public final boolean remove(final Object o) {
				return set.remove(encode((java.util.Map.Entry<K, V>) o));
			}
			@SuppressWarnings("unchecked")
			@Override
			public boolean containsAll(final Collection<?> c) {
				final ArrayList<java.util.Map.Entry<byte[], byte[]>> col = new ArrayList<java.util.Map.Entry<byte[], byte[]>>(c.size());

				for(final Object o : c)
					col.add(encode((java.util.Map.Entry<K, V>) o));

				return set.containsAll(col);
			}

			@SuppressWarnings("unchecked")
			@Override
			public final boolean addAll(Collection<? extends java.util.Map.Entry<K, V>> c) {
				final ArrayList<java.util.Map.Entry<byte[], byte[]>> col = new ArrayList<java.util.Map.Entry<byte[], byte[]>>(c.size());

				for(final Object o : c)
					col.add(encode((java.util.Map.Entry<K, V>) o));

				return set.addAll(col);
			}
			@SuppressWarnings("unchecked")
			@Override
			public final boolean retainAll(final Collection<?> c) {
				final ArrayList<java.util.Map.Entry<byte[], byte[]>> col = new ArrayList<java.util.Map.Entry<byte[], byte[]>>(c.size());

				for(final Object o : c)
					col.add(encode((java.util.Map.Entry<K, V>) o));

				return set.retainAll(col);
			}
			@SuppressWarnings("unchecked")
			@Override
			public final boolean removeAll(Collection<?> c) {
				final ArrayList<java.util.Map.Entry<byte[], byte[]>> col = new ArrayList<java.util.Map.Entry<byte[], byte[]>>(c.size());

				for(final Object o : c)
					col.add(encode((java.util.Map.Entry<K, V>) o));

				return set.removeAll(col);
			}
			@Override
			public final void clear() {
				set.clear();
			}
		};
	}

}
