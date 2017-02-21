package com.gf.persistance.impl.maps;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.gf.persistance.PersistanceFactory;
import com.gf.persistance.PersistedLongList;
import com.gf.persistance.PersistedLongMap;
import com.gf.persistance.impl.CollectionsAccessBuffer;
import com.gf.util.string.MacroCompiler;

public final class ByteArrayLongMap implements PersistedLongMap<byte[], byte[]>{
	private final File storage_file;
	private final RandomAccessFile ra_file;
	private final int gc_balance;
	private final int gc_lower_balance;
	private final MappedByteBuffer offsetBuffer;
	private volatile long _offset;
	private final HashMap<Long, CollectionsAccessBuffer> buffers;
	private final PersistedLongList<Long> index_list;
	private volatile long _size;
	private final MappedByteBuffer sizeBuffer;
	private final long capacity;

	public ByteArrayLongMap(final File index_file, final File storage_file, final int gc_balance, final long capacity){
		this.capacity = capacity;
		this.storage_file = storage_file;
		this.gc_balance = gc_balance;
		this.gc_lower_balance = gc_balance/2;
		this.buffers = new HashMap<Long, CollectionsAccessBuffer>();
		this.index_list = PersistanceFactory.createLongList(index_file, storage_file, gc_balance, Long.class);

		if (!storage_file.exists()){
			try {
				storage_file.createNewFile();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			try {
				this.ra_file = new RandomAccessFile(storage_file, "rw");
				this.offsetBuffer = ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Long.BYTES);
				this.offsetBuffer.position(0);
				this.offsetBuffer.putLong(new Integer(Long.BYTES).longValue());
				this._offset = new Integer(Long.BYTES + Long.BYTES).longValue();

				this.sizeBuffer = ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, Long.BYTES, Long.BYTES);
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
			this._size = 0;
			
			final HashMap<String, String> params = new HashMap<String, String>();
			for (long i = 0; i < capacity; i++) {
				index_list.add((long) -1);
				if (i % 10000 == 0){
					params.put("progress", Long.toString(i));
					params.put("total", Long.toString(capacity));
					params.put("percent", Double.toString(((double)i/(double)capacity) * 100.00));
					System.out.print(MacroCompiler.compile("Initializing long map index file. Done ${progress} out of ${total}. Percent: ${percent}%\r", params));
				}
			}
			setSize(0);
		}else{
			try {
				this.ra_file = new RandomAccessFile(storage_file, "rw");
				this.offsetBuffer = ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Long.BYTES);
				this.offsetBuffer.position(0);
				this._offset = this.offsetBuffer.getLong();
				this.sizeBuffer = ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, Long.BYTES, Long.BYTES);
				this.sizeBuffer.position(0);
				this._size = sizeBuffer.getLong();
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private final void setSize(final long size){
		sizeBuffer.position(0);
		sizeBuffer.putLong(size);
	}

	private final long incrementSize(final long delta){
		final long result = _size;
		_size = _size + delta;
		this.setSize(_size);
		return result;
	}
	private final CollectionsAccessBuffer getBuffer(final long range){
		try{
			CollectionsAccessBuffer result = buffers.get(range);
			if (result == null){
				final int tmpSize = Entry.calculateSize(0);
				final MappedByteBuffer buf = ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, range, tmpSize);
				buf.position(Entry.sizePosition);
				final int entitySize = Entry.calculateSize(buf.getInt() + buf.getInt());

				final CollectionsAccessBuffer candidate = new CollectionsAccessBuffer(ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, range, entitySize), entitySize);
				result = buffers.putIfAbsent(range, candidate);
				if (result == null){
					result = candidate;
					gc_if_needed();
				}else{
					candidate.dispose();
					result.lastAccess.set(System.currentTimeMillis());
				}
			}else{
				result.lastAccess.set(System.currentTimeMillis());
			}
			return result;
		}catch(final Throwable t){
			throw new RuntimeException("Failed to get range: " + range,t);
		}
	}
	private final long incrementOffset(final long delta){
		final long result = _offset;
		_offset = _offset + delta;
		this.setOffset(_offset);
		return result;
	}
	private final void setOffset(final long size){
		offsetBuffer.position(0);
		offsetBuffer.putLong(size);
	}
	private final Object[] allocateBuffer(final int size){
		final long range = incrementOffset(Integer.valueOf(size).longValue());
		try{
			final CollectionsAccessBuffer result = new CollectionsAccessBuffer(ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, range, size), size);
			result.lastAccess.set(System.currentTimeMillis());
			buffers.put(range, result);
			gc_if_needed();
			return new Object[]{result, range};
		}catch(final Throwable t){
			throw new RuntimeException("Failed to alocate range: " + range + " of size " + size, t);
		}
	}
	private final void gc_if_needed(){
		if (buffers.size() > gc_balance){
			final int toRemove = gc_balance - gc_lower_balance;
			final ArrayList<Long> keys = new ArrayList<Long>(buffers.size());

			for(final Long key : buffers.keySet())
				keys.add(key);

			keys.sort(new Comparator<Long>() {
				@Override
				public final int compare(final Long key1, final Long key2) {
					return Long.compare(buffers.get(key1).lastAccess.get(), buffers.get(key2).lastAccess.get());
				}
			});

			int index = keys.size() - 1;
			for (int i = 0; i < toRemove; i++) {
				final CollectionsAccessBuffer buf = buffers.remove(keys.get(index));

				if (buf != null)
					buf.dispose();

				index--;
			}
		}
	}
	private static final boolean arrayEquals(final byte[] a1, final byte[]  a2){
		if (a1 == null)
			if (a2 == null)
				return true;
			else
				return false;
		else
			if (a2 == null)
				return false;
			else{
				final int len = a1.length;
				if (len != a2.length)
					return false;
				else{
					for (int i = 0; i < a1.length; i++) 
						if (a1[i] != a2[i])
							return false;

					return true;
				}
			}
	}


	@Override
	public final File getIndexFile() {
		return index_list.getIndexFile();
	}

	@Override
	public final File getStorageFile() {
		return storage_file;
	}

	@Override
	public final void delete() {
		try{this.clear();}catch(final Throwable t){}
		try{this.close();}catch(final Throwable t){}
		try{this.index_list.delete();}catch(final Throwable t){}
		try{this.storage_file.delete();}catch(final Throwable t){}
	}

	@Override
	public final void close() throws IOException {
		try{ra_file.close();}catch(final Throwable t){}
		try{index_list.close();}catch(final Throwable t){}
	}

	private static final int hash(final byte[] key) {
		int h;
		return (h = Arrays.hashCode(key)) ^ (h >>> 16);
	}
	private final int getIndex(final byte[] key){
		final int hash = hash(key);
		return (Long.valueOf(capacity).intValue() - 1) & hash;
	}

	private final Entry getEntry(final long pointer){
		if (pointer < 0)
			return null;

		return new Entry(getBuffer(pointer));
	}
	@Override
	public final byte[] put(final byte[] key, final byte[] value) {
		final int index = getIndex(key);
		final long pointer = index_list.get(index);
		if (pointer < 0){
			final int key_value_len = value.length + key.length;
			final Object[] arr = allocateBuffer(Entry.calculateSize(key_value_len));
			final CollectionsAccessBuffer buf = (CollectionsAccessBuffer) arr[0];
			final Long newPointer = (Long) arr[1];
			new Entry(buf, value, key);
			index_list.set(index, newPointer);
			this.incrementSize(1);
			return null;
		}else{
			Entry entry = null;
			long next = pointer;
			while((entry = getEntry(next)) != null){
				final long prev = next;
				next = entry.getNextIndex();
				if (arrayEquals(key, entry.getKey())){
					final byte[] result = entry.getValue();
					if (value.length <= result.length){
						entry.setValue(value);
					}else{
						buffers.remove(prev);
						final int key_value_len = value.length + key.length;
						final Object[] arr = allocateBuffer(Entry.calculateSize(key_value_len));
						final CollectionsAccessBuffer buf = (CollectionsAccessBuffer) arr[0];
						final Long newPointer = (Long) arr[1];
						final Entry newEntry = new Entry(buf, value, key);
						if (entry.getPreviousIndex() < 0){
							if (next < 0){
								index_list.set(index, newPointer);
							}else{
								newEntry.setNext(next);
								index_list.set(index, newPointer);
							}
						}else{
							newEntry.setPrevious(entry.getPreviousIndex());
							if (next < 0){
								index_list.set(index, newPointer);
							}else{
								newEntry.setNext(next);
								index_list.set(index, newPointer);
							}
						}
						entry.dispose();
					}
					return result;
				}else{
					if (next < 0){
						final int key_value_len = value.length + key.length;
						final Object[] arr = allocateBuffer(Entry.calculateSize(key_value_len));
						final CollectionsAccessBuffer buf = (CollectionsAccessBuffer) arr[0];
						final Long newPointer = (Long) arr[1];
						final Entry newEntry = new Entry(buf, value, key);
						entry.setNext(newPointer);
						newEntry.setPrevious(prev);
						this.incrementSize(1);
						return null;
					}
				}
			}
			throw new RuntimeException("Inconsistent index. While range was positive, no key was found.");
		}
	}
	@Override
	public final byte[] get(final Object obj_key) {
		if (obj_key == null)
			throw new NullPointerException("Unlike an JDK map, persisted map can not accept null keys or null values.");

		if (!(obj_key instanceof byte[]))
			throw new RuntimeException("Got key of type '" +obj_key.getClass().getName() + "' but was expecting '" + byte[].class.getName() + "'.");

		final byte[] key = (byte[]) obj_key;
		final int index = getIndex(key);
		final long pointer = index_list.get(index);
		if (pointer < 0)
			return null;

		Entry entry = null;
		long next = pointer;
		while((entry = getEntry(next)) != null){
			if (arrayEquals(key, entry.getKey()))
				return entry.getValue();
			else
				next = entry.getNextIndex();
		}
		return null;
	}

	@Override
	public final boolean isEmpty() {
		return this.size() == 0;
	}

	@Override
	public final boolean containsKey(final Object obj_key) {
		if (obj_key == null)
			throw new NullPointerException("Unlike an JDK map, persisted map can not accept null keys or null values.");

		if (!(obj_key instanceof byte[]))
			throw new RuntimeException("Got key of type '" +obj_key.getClass().getName() + "' but was expecting '" + byte[].class.getName() + "'.");

		final byte[] key = (byte[]) obj_key;
		final int index = getIndex(key);
		final long pointer = index_list.get(index);
		if (pointer < 0)
			return false;

		Entry entry = null;
		long next = pointer;
		while((entry = getEntry(next)) != null){
			if (arrayEquals(key, entry.getKey()))
				return true;
			else
				next = entry.getNextIndex();
		}
		return false;
	}

	@Override
	public final boolean containsValue(final Object value) {
		if (value == null)
			throw new NullPointerException("Unlike an JDK map, persisted map can not accept null keys or null values.");

		if (!(value instanceof byte[]))
			throw new RuntimeException("Got value of type '" +value.getClass().getName() + "' but was expecting '" + byte[].class.getName() + "'.");

		for(final byte[] key : keySet())
			if (containsKey(key))
				return true;

		return false;
	}

	@Override
	public final long size() {
		return _size;
	}


	@Override
	public final byte[] remove(final Object obj_key) {
		if (obj_key == null)
			throw new NullPointerException("Unlike an JDK map, persisted map can not accept null keys or null values.");

		if (!(obj_key instanceof byte[]))
			throw new RuntimeException("Got value of type '" + obj_key.getClass().getName() + "' but was expecting '" + byte[].class.getName() + "'.");

		final byte[] key = (byte[]) obj_key;
		final int index = getIndex(key);
		final long pointer = index_list.get(index);
		if (pointer < 0)
			return null;

		Entry entry = null;
		long next = pointer;
		while((entry = getEntry(next)) != null){
			if (arrayEquals(key, entry.getKey())){
				buffers.remove(next);
				final byte[] result = entry.getValue();
				final long prev = entry.getPreviousIndex();
				final long nxt = entry.getNextIndex();
				if (prev < 0){
					if (nxt < 0){
						index_list.set(index, Long.valueOf(-1));
					}else{
						final Entry nextEntry = getEntry(nxt);
						nextEntry.setPrevious(prev);
						index_list.set(index, nxt);
					}
				}else{
					if (nxt < 0){
						final Entry prevEntry = getEntry(prev);
						prevEntry.setNext(nxt);
					}else{
						final Entry prevEntry = getEntry(prev);
						final Entry nextEntry = getEntry(nxt);
						prevEntry.setNext(nxt);
						nextEntry.setPrevious(prev);
					}
				}
				incrementSize(-1);
				return result;
			}else
				next = entry.getNextIndex();
		}
		return null;
	}

	@Override
	public final void putAll(final Map<? extends byte[], ? extends byte[]> m) {
		for(final byte[] key : m.keySet())
			this.put(key, m.get(key));
	}

	@Override
	public final void clear() {
		this.setOffset(new Integer(Long.BYTES + Long.BYTES).longValue());
		this.setSize(0);

		for (long i = 0; i < capacity; i++)
			index_list.set(i, (long) -1);
	}

	@Override
	public final Set<byte[]> keySet() {
		return new Set<byte[]>() {
			@SuppressWarnings("unchecked")
			@Override
			public final <T> T[] toArray(final T[] a) {
				final Iterator<byte[]> iter = this.iterator();
				int index = 0, len = a.length;
				while(iter.hasNext()){
					if (index < len){
						a[index] = (T) iter.next();
					}else{
						return a;
					}
					index++;
				}
				return a;
			}
			@Override
			public final Object[] toArray() {
				final byte[][] arr = new byte[this.size()][];
				return toArray(arr);
			}
			@Override
			public final int size() {
				return (int) ByteArrayLongMap.this.size();
			}
			@Override
			public final boolean retainAll(final Collection<?> c) {
				final HashSet<Object> set = new HashSet<Object>();
				set.addAll(c);
				final ArrayList<byte[]> toRemove = new ArrayList<byte[]>();

				for(final byte[] o : this)
					if (!set.contains(o))
						toRemove.add(o);

				return this.removeAll(toRemove);
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
			public final boolean remove(final Object o) {
				if (o == null)
					return false;

				if (o instanceof byte[])
					if (ByteArrayLongMap.this.remove((byte[])o) == null)
						return false;
					else
						return true;

				return false;
			}

			@Override
			public final Iterator<byte[]> iterator() {
				return new Iterator<byte[]>() {
					private final Iterator<Long> iter = ByteArrayLongMap.this.index_list.iterator();
					private volatile Entry currentEntry = null;

					@Override
					public final byte[] next() {
						Entry entry = currentEntry;
						if (entry == null){
							if (hasNext()){
								entry = currentEntry;
								if (entry == null){
									return null;
								}else{
									final long nxt = entry.getNextIndex();
									if (nxt < 0){
										currentEntry = null;
									}else{
										currentEntry = ByteArrayLongMap.this.getEntry(nxt);
									}
									return entry.getKey();
								}
							}
						}else{
							final long nxt = entry.getNextIndex();
							if (nxt < 0){
								currentEntry = null;
							}else{
								currentEntry = ByteArrayLongMap.this.getEntry(nxt);
							}
							return entry.getKey();
						}
						return null;
					}
					@Override
					public final boolean hasNext() {
						if (currentEntry == null){
							while(iter.hasNext()){
								final long next = iter.next();
								if (next >= 0){
									currentEntry = ByteArrayLongMap.this.getEntry(next);
									return true;
								}
							}
							return false;
						}else{
							return true;
						}
					}
				};
			}
			@Override
			public final boolean isEmpty() {
				return ByteArrayLongMap.this.isEmpty();
			}
			@Override
			public final boolean containsAll(final Collection<?> c) {
				if (c == null)
					return false;

				for(final Object o : c)
					if (!contains(o))
						return false;

				return true;
			}
			@Override
			public final boolean contains(final Object o) {
				return ByteArrayLongMap.this.containsKey(o);
			}
			@Override
			public final void clear() {
				ByteArrayLongMap.this.clear();
			}
			@Override
			public final boolean addAll(final Collection<? extends byte[]> c) {
				throw new RuntimeException("Adding not supported on key set derived from map.");
			}
			@Override
			public final boolean add(final byte[] e) {
				throw new RuntimeException("Adding not supported on key set derived from map.");
			}
		};
	}

	@Override
	public final Collection<byte[]> values() {
		return new Collection<byte[]>() {
			@SuppressWarnings("unchecked")
			@Override
			public final <T> T[] toArray(final T[] a) {
				int index = 0, len = a.length;
				for(final java.util.Map.Entry<byte[], byte[]> ent : ByteArrayLongMap.this.entrySet()){
					if (index < len){
						a[index] = (T) ent.getValue();
					}else{
						return a;
					}
					index++;
				}
				return a;
			}

			@Override
			public final Object[] toArray() {
				final byte[][] res = new byte[this.size()][];
				return toArray(res);
			}

			@Override
			public final int size() {
				return (int) ByteArrayLongMap.this.size();
			}

			@Override
			public final boolean retainAll(final Collection<?> c) {
				final HashSet<Object> set = new HashSet<Object>();
				set.addAll(c);
				final ArrayList<byte[]> toRemove = new ArrayList<byte[]>();

				for(final java.util.Map.Entry<byte[], byte[]> entry : ByteArrayLongMap.this.entrySet())
					if (!set.contains(entry.getValue()))
						toRemove.add(entry.getKey());

				return removeAllKeys(toRemove);
			}
			private final boolean removeAllKeys(final ArrayList<byte[]> list){
				boolean result = false;
				for(final byte[] key : list){
					if (ByteArrayLongMap.this.remove(key) != null){
						result = true;
					}
				}
				return result;
			}
			@Override
			public final boolean removeAll(final Collection<?> c) {
				final ArrayList<byte[]> toRemove = new ArrayList<byte[]>();

				for(final java.util.Map.Entry<byte[], byte[]> entry : ByteArrayLongMap.this.entrySet())
					if (c.contains(entry.getValue()))
						toRemove.add(entry.getKey());

				return removeAllKeys(toRemove);
			}

			@Override
			public final boolean remove(final Object o) {
				if (o == null)
					return false;

				if (o instanceof byte[]){
					final ArrayList<byte[]> toRemove = new ArrayList<byte[]>();
					final byte[] key = (byte[])o;

					for(final java.util.Map.Entry<byte[], byte[]> entry : ByteArrayLongMap.this.entrySet())
						if (arrayEquals(key, entry.getValue()))
							toRemove.add(entry.getKey());

					return removeAllKeys(toRemove);
				}
				return false;
			}

			@Override
			public final Iterator<byte[]> iterator() {
				return new Iterator<byte[]>() {
					private final Iterator<java.util.Map.Entry<byte[], byte[]>> iter = ByteArrayLongMap.this.entrySet().iterator();
					@Override
					public final byte[] next() {
						final java.util.Map.Entry<byte[], byte[]> ent = iter.next();

						if (ent == null)
							return null;

						return ent.getValue();
					}
					@Override
					public final boolean hasNext() {
						return iter.hasNext();
					}
				};
			}

			@Override
			public final boolean isEmpty() {
				return ByteArrayLongMap.this.isEmpty();
			}

			@Override
			public final boolean containsAll(final Collection<?> c) {
				for(final Object o : c)
					if (!contains(o))
						return false;

				return true;
			}

			@Override
			public final boolean contains(final Object o) {
				if (o == null)
					return false;

				if (o instanceof byte[]){
					final byte[] obj = (byte[]) o;
					final Iterator<byte[]> iter = iterator();

					while(iter.hasNext())
						if (arrayEquals(iter.next(), obj))
							return true;
				}
				return false;
			}

			@Override
			public void clear() {
				ByteArrayLongMap.this.clear();
			}

			@Override
			public final boolean addAll(Collection<? extends byte[]> c) {
				throw new RuntimeException("Adding not supported on value set derived from map.");
			}

			@Override
			public final boolean add(byte[] e) {
				throw new RuntimeException("Adding not supported on value set derived from map.");
			}
		};
	}

	@Override
	public Set<java.util.Map.Entry<byte[], byte[]>> entrySet() {
		return new Set<java.util.Map.Entry<byte[], byte[]>>() {
			@SuppressWarnings("unchecked")
			@Override
			public final <T> T[] toArray(final T[] a) {
				final Iterator<java.util.Map.Entry<byte[], byte[]>> iter = this.iterator();
				int index = 0, len = a.length;
				while(iter.hasNext()){
					if (index < len){
						a[index] = (T) iter.next();
					}else{
						return a;
					}
					index++;
				}
				return a;
			}
			@Override
			public final Object[] toArray() {
				@SuppressWarnings("unchecked")
				final java.util.Map.Entry<byte[], byte[]>[] arr = new java.util.Map.Entry[this.size()];
				return toArray(arr);
			}
			@Override
			public final int size() {
				return (int) ByteArrayLongMap.this.size();
			}
			@Override
			public final boolean retainAll(final Collection<?> c) {
				final HashSet<Object> set = new HashSet<Object>();
				set.addAll(c);
				final ArrayList<java.util.Map.Entry<byte[], byte[]>> toRemove = new ArrayList<java.util.Map.Entry<byte[], byte[]>>();

				for(final java.util.Map.Entry<byte[], byte[]> o : this)
					if (!set.contains(o))
						toRemove.add(o);

				return this.removeAll(toRemove);
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
			@SuppressWarnings("unchecked")
			@Override
			public final boolean remove(final Object o) {
				if (o == null)
					return false;

				if (o instanceof java.util.Map.Entry)
					if (ByteArrayLongMap.this.remove(((java.util.Map.Entry<byte[], byte[]>)o).getKey()) == null)
						return false;
					else
						return true;

				return false;
			}

			@Override
			public final Iterator<java.util.Map.Entry<byte[], byte[]>> iterator() {
				return new Iterator<java.util.Map.Entry<byte[], byte[]>>() {
					private final Iterator<Long> iter = ByteArrayLongMap.this.index_list.iterator();
					private volatile Entry currentEntry = null;

					@Override
					public final java.util.Map.Entry<byte[], byte[]> next() {
						Entry entry = currentEntry;
						if (entry == null){
							if (hasNext()){
								entry = currentEntry;
								if (entry == null){
									return null;
								}else{
									final long nxt = entry.getNextIndex();
									if (nxt < 0){
										currentEntry = null;
									}else{
										currentEntry = ByteArrayLongMap.this.getEntry(nxt);
									}
									final Entry f_entry = entry;
									return new java.util.Map.Entry<byte[], byte[]>() {
										private final byte[] k = f_entry.getKey();
										@Override
										public final byte[] setValue(final byte[] value) {
											return ByteArrayLongMap.this.put(k, value);
										}

										@Override
										public final byte[] getValue() {
											return ByteArrayLongMap.this.get(k);
										}

										@Override
										public final byte[] getKey() {
											return k;
										}
									};
								}
							}
						}else{
							final long nxt = entry.getNextIndex();
							if (nxt < 0){
								currentEntry = null;
							}else{
								currentEntry = ByteArrayLongMap.this.getEntry(nxt);
							}
							final Entry f_entry = entry;
							return new java.util.Map.Entry<byte[], byte[]>() {
								private final byte[] k = f_entry.getKey();
								@Override
								public final byte[] setValue(final byte[] value) {
									return ByteArrayLongMap.this.put(k, value);
								}

								@Override
								public final byte[] getValue() {
									return ByteArrayLongMap.this.get(k);
								}

								@Override
								public final byte[] getKey() {
									return k;
								}
							};
						}
						return null;
					}
					@Override
					public final boolean hasNext() {
						if (currentEntry == null){
							while(iter.hasNext()){
								final long next = iter.next();
								if (next >= 0){
									currentEntry = ByteArrayLongMap.this.getEntry(next);
									return true;
								}
							}
							return false;
						}else{
							return true;
						}
					}
				};
			}
			@Override
			public final boolean isEmpty() {
				return ByteArrayLongMap.this.isEmpty();
			}
			@Override
			public final boolean containsAll(final Collection<?> c) {
				if (c == null)
					return false;

				for(final Object o : c)
					if (!contains(o))
						return false;

				return true;
			}
			@Override
			public final boolean contains(final Object o) {
				return ByteArrayLongMap.this.containsKey(o);
			}
			@Override
			public final void clear() {
				ByteArrayLongMap.this.clear();
			}
			@Override
			public final boolean addAll(final Collection<? extends java.util.Map.Entry<byte[], byte[]>> c) {
				throw new RuntimeException("Adding not supported on set derived from map.");
			}
			@Override
			public final boolean add(final java.util.Map.Entry<byte[], byte[]> e) {
				final byte[] val =e.getValue();
				final byte[] ret = ByteArrayLongMap.this.put(e.getKey(), e.getValue());
				return !arrayEquals(val, ret);
			}
		};
	}









	//=======================================================================================
	private static final class Entry{
		private final CollectionsAccessBuffer buffer;
		public static final int nextPosition = 0;
		public static final int prevPosition = Long.BYTES;
		public static final int sizePosition = prevPosition + Long.BYTES;
		public static final int keySizePosition = sizePosition + Integer.BYTES;
		public static final int contentPosition = keySizePosition + Integer.BYTES;
		private volatile long next;
		private volatile long previous;
		private volatile int valueSize;
		private volatile int keySize;

		public static final int calculateSize(final int size){
			return size + contentPosition;
		}


		public Entry(final CollectionsAccessBuffer buffer){
			this.buffer = buffer;
			this.buffer.buffer.position(nextPosition);
			this.next = this.buffer.buffer.getLong();
			this.previous = this.buffer.buffer.getLong();
			this.valueSize = this.buffer.buffer.getInt();
			this.keySize = this.buffer.buffer.getInt();
		}
		public Entry(final CollectionsAccessBuffer buffer, final byte[] value, final byte[] key){
			this.buffer = buffer;
			final long nxt = -1;
			final long prv = -1;
			final int sz = value.length;
			final int ksz = key.length;
			this.buffer.buffer.position(nextPosition);
			this.buffer.buffer.putLong(nxt);
			this.buffer.buffer.putLong(prv);
			this.buffer.buffer.putInt(sz);
			this.buffer.buffer.putInt(ksz);
			this.buffer.buffer.put(value);
			this.buffer.buffer.put(key);
			this.next = nxt;
			this.previous = prv;
			this.valueSize = sz;
			this.keySize = ksz;
		}

		public final void dispose(){
			this.buffer.dispose();
		}
		public final byte[] getKey(){
			final byte[] result = new byte[this.keySize];
			this.buffer.buffer.position(contentPosition + valueSize);
			this.buffer.buffer.get(result);
			return result;
		}
		public final byte[] getValue(){
			final byte[] result = new byte[this.valueSize];
			this.buffer.buffer.position(contentPosition);
			this.buffer.buffer.get(result);
			return result;
		}
		public final void setValue(final byte[] content){
			final int newSize = content.length;
			if (newSize == valueSize){
				this.buffer.buffer.position(contentPosition);
				this.buffer.buffer.put(content);
			}else{
				this.buffer.buffer.position(sizePosition);
				this.buffer.buffer.putInt(newSize);
				this.buffer.buffer.position(contentPosition);
				this.buffer.buffer.put(content);
			}
		}
		public final long setNext(final long index){
			final long prev = next;
			next = index;
			if (prev != index){
				this.buffer.buffer.position(nextPosition);
				this.buffer.buffer.putLong(index);
			}
			return prev;
		}
		public final long setPrevious(final long index){
			final long prev = previous;
			previous = index;
			if (prev != index){
				this.buffer.buffer.position(prevPosition);
				this.buffer.buffer.putLong(index);
			}
			return prev;
		}
		public final long getNextIndex(){
			return next;
		}
		public final long getPreviousIndex(){
			return previous;
		}
		@Override
		public final String toString() {
			return "Entry [buffer=" + buffer + ", next=" + next + ", previous=" + previous + ", valueSize=" + valueSize
					+ ", keySize=" + keySize + "]";
		}
		@Override
		public final int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((buffer == null) ? 0 : buffer.hashCode());
			return result;
		}
		@Override
		public final boolean equals(final Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final Entry other = (Entry) obj;
			if (buffer == null) {
				if (other.buffer != null)
					return false;
			} else if (!buffer.equals(other.buffer))
				return false;
			return true;
		}
	}
}
