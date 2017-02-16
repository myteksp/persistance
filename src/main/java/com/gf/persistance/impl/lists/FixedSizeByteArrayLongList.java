package com.gf.persistance.impl.lists;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.gf.persistance.LongListIterator;
import com.gf.persistance.PersistedLongList;
import com.gf.persistance.impl.CollectionsAccessBuffer;

public final class FixedSizeByteArrayLongList implements PersistedLongList<byte[]>{
	private final int ENTRY_SIZE;

	private final File file;
	private final RandomAccessFile ra_file;
	private final MappedByteBuffer sizeBuffer;
	private final HashMap<Long, CollectionsAccessBuffer> buffers;
	private final int gc_balance;
	private final int gc_lower_balance;
	private volatile long _size;

	public FixedSizeByteArrayLongList(final File file, final int entitySize, final int gc_balance){
		if (file == null)
			throw new NullPointerException();

		this.file = file;
		this.gc_balance = gc_balance;
		this.gc_lower_balance = gc_balance/2;
		this.buffers = new HashMap<Long, CollectionsAccessBuffer>();
		this.ENTRY_SIZE = entitySize;

		if (!file.exists()){
			try {
				file.createNewFile();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			try {
				this.ra_file = new RandomAccessFile(file, "rw");
				this.sizeBuffer = ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Long.BYTES);
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
			this._size = 0;
			setSize(Long.valueOf(0).longValue());
		}else{
			try {
				this.ra_file = new RandomAccessFile(file, "rw");
				this.sizeBuffer = ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Long.BYTES);
				this.sizeBuffer.position(0);
				this._size = sizeBuffer.getLong();
			} catch (final Exception e) {
				throw new RuntimeException(e);
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

	private final void setSize(final Long size){
		_size = size;
		sizeBuffer.position(0);
		sizeBuffer.putLong(size);
	}

	private final long incrementSize(final long delta){
		final long result = _size;
		_size = _size + delta;
		this.setSize(_size);
		return result;
	}

	private final CollectionsAccessBuffer getBuffer(final long index){
		try{
			CollectionsAccessBuffer result = buffers.get(index);
			if (result == null){
				final CollectionsAccessBuffer candidate = new CollectionsAccessBuffer(ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, Long.BYTES + (index * ENTRY_SIZE), ENTRY_SIZE), ENTRY_SIZE);
				result = buffers.putIfAbsent(index, candidate);
				if (result == null){
					result = candidate;
					gc_if_needed();
				}else{
					candidate.dispose();
				}
			}else{
				result.lastAccess.set(System.currentTimeMillis());
			}
			return result;
		}catch(final Throwable t){
			throw new RuntimeException(t);
		}
	}

	@Override
	public final File getIndexFile() {
		return file;
	}

	@Override
	public final File getStorageFile() {
		return null;
	}

	@Override
	public final void delete() {
		try{this.clear();}catch(final Throwable t){}
		try{this.close();}catch(final Throwable t){}
		try{this.file.delete();}catch(final Throwable t){}
	}

	@Override
	public final void close() throws IOException {
		buffers.clear();
		try {ra_file.close();} catch (final Exception e) {}
	}

	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}

	@Override
	public final long size() {
		return _size;
	}

	@Override
	public final boolean isEmpty() {
		return _size == 0;
	}

	@Override
	public final boolean contains(Object o) {
		if (o == null)
			return false;

		if (o instanceof byte[]){
			final byte[] O = (byte[]) o;
			for(final byte[] item : this)
				if (arrayEquals(item, O))
					return true;
		}

		return false;
	}

	@Override
	public final Iterator<byte[]> iterator() {
		return new Iterator<byte[]>() {
			private volatile long index = 0;
			private volatile byte[] next = null;

			@Override
			public final byte[] next() {
				if (next != null){
					final byte[] res = next;
					next = null;
					index++;
					return res;
				}
				final long g = index;
				index++;
				return get(g);
			}

			@Override
			public final boolean hasNext() {
				final long i = index;
				final boolean result = i < size();
				if (result)
					if (next == null)
						next = get(i);

				return result;
			}
		};
	}

	@Override
	public final Object[] toArray() {
		final byte[][] arr = new byte[Long.valueOf(this.size()).intValue()][];
		return this.toArray(arr);
	}

	@Override
	public final byte[][] toArray(final byte[][] a) {
		final long size = this.size();
		final long len = Long.valueOf(a.length).longValue();
		for (long i = 0; i < size; i++) 
			if (i < len)
				a[Long.valueOf(i).intValue()] = this.get(i);
			else
				return a;

		return a;
	}

	@Override
	public final boolean add(final byte[] e) {
		final CollectionsAccessBuffer buf = getBuffer(incrementSize(1));
		buf.buffer.position(0);
		buf.buffer.put(e);
		return true;
	}

	@Override
	public final boolean remove(final Object o) {
		if (o == null)
			return false;

		final ArrayList<Long> indexes = new ArrayList<Long>();
		if (o instanceof byte[]){
			final byte[] O = (byte[]) o;
			final long len = size();
			for (long i = 0; i < len; i++) {
				final byte[] item = get(i);
				if (arrayEquals(item, O)){
					indexes.add(i);
				}
			}
			for(final long ind : indexes){
				remove(ind);
			}
		}

		return indexes.size() > 0;
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
	public final boolean addAll(final Collection<? extends byte[]> c) {
		for(final byte[] o : c)
			add(o);

		return true;
	}

	@Override
	public final boolean addAll(final long index, final Collection<? extends byte[]> c) {
		long currentIndex = index;
		final long size = size();
		for(final byte[] item : c){
			if (currentIndex < size){
				set(currentIndex, item);
			}else{
				add(item);
			}
		}
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
	public final void clear() {
		setSize(Long.valueOf(0).longValue());
	}

	@Override
	public final byte[] get(final long index) {
		final long size = size();
		if (index < size){
			final CollectionsAccessBuffer buf = getBuffer(index);
			final byte[] res = new byte[ENTRY_SIZE];
			buf.buffer.position(0);
			buf.buffer.get(res);
			return res;
		}
		throw new ArrayIndexOutOfBoundsException("Trying to access index " + index + " in a list of length of " + size);
	}

	@Override
	public final byte[] set(final long index, final byte[] element) {
		final long size = size();
		if (index < size){
			final CollectionsAccessBuffer buf = getBuffer(index);
			final byte[] res = new byte[ENTRY_SIZE];
			buf.buffer.position(0);
			buf.buffer.get(res);
			buf.buffer.position(0);
			buf.buffer.put(element);
			return res;
		}else{
			final long newSize = index + 1;
			setSize(newSize);
			for(long i = size; i < newSize; i++){
				final CollectionsAccessBuffer buf = getBuffer(i);
				buf.buffer.position(0);
				buf.buffer.put(element);
			}
			return null;
		}
	}

	@Override
	public final void add(final long index, final byte[] element) {
		set(index, element);
	}

	@Override
	public final long indexOf(final Object o) {
		if (o instanceof byte[]){
			final byte[] O = (byte[]) o;
			final long len = size();
			for (int i = 0; i < len; i++) 
				if (arrayEquals(get(i), O))
					return i;
		}
		return -1;
	}

	@Override
	public final byte[] remove(final long index) {
		final long size = size();
		if (index < size){
			final byte[] result = get(index);
			final long start = index + 1;

			if (start < size)
				for (long i = start; i < size; i++) 
					set(i-1, get(i));

			incrementSize(-1);
			return result;
		}
		return null;
	}

	@Override
	public final long lastIndexOf(final Object o) {
		if (o instanceof byte[]){
			final byte[] O = (byte[]) o;

			for(long i = size() - 1; i > -1; i--)
				if (arrayEquals(get(i), O))
					return i;

		}
		return -1;
	}

	@Override
	public final LongListIterator<byte[]> listIterator() {
		return new LongListIterator<byte[]>() {
			private volatile long currentIndex = 0;
			@Override
			public final void set(final byte[] e) {
				FixedSizeByteArrayLongList.this.set(currentIndex, e);
			}
			@Override
			public final void remove() {
				FixedSizeByteArrayLongList.this.remove(currentIndex);
			}
			@Override
			public final long previousIndex() {
				return currentIndex - 1;
			}
			@Override
			public final byte[] previous() {
				currentIndex--;
				return FixedSizeByteArrayLongList.this.get(currentIndex);
			}
			@Override
			public final long nextIndex() {
				return currentIndex + 1;
			}
			@Override
			public final byte[] next() {
				currentIndex++;
				return FixedSizeByteArrayLongList.this.get(currentIndex);
			}
			@Override
			public final boolean hasPrevious() {
				return currentIndex > 0;
			}
			@Override
			public final boolean hasNext() {
				return currentIndex < (FixedSizeByteArrayLongList.this.size() - 1);
			}
			@Override
			public final void add(final byte[] e) {
				FixedSizeByteArrayLongList.this.add(currentIndex, e);
				currentIndex++;
			}
		};
	}

	@Override
	public final LongListIterator<byte[]> listIterator(final long index) {
		return new LongListIterator<byte[]>() {
			private volatile long currentIndex = index;
			@Override
			public final void set(final byte[] e) {
				FixedSizeByteArrayLongList.this.set(currentIndex, e);
			}
			@Override
			public final void remove() {
				FixedSizeByteArrayLongList.this.remove(currentIndex);
			}
			@Override
			public final long previousIndex() {
				return currentIndex - 1;
			}
			@Override
			public final byte[] previous() {
				currentIndex--;
				return FixedSizeByteArrayLongList.this.get(currentIndex);
			}
			@Override
			public final long nextIndex() {
				return currentIndex + 1;
			}
			@Override
			public final byte[] next() {
				currentIndex++;
				return FixedSizeByteArrayLongList.this.get(currentIndex);
			}
			@Override
			public final boolean hasPrevious() {
				return currentIndex > 0;
			}
			@Override
			public final boolean hasNext() {
				return currentIndex < (FixedSizeByteArrayLongList.this.size() - 1);
			}
			@Override
			public final void add(final byte[] e) {
				FixedSizeByteArrayLongList.this.add(currentIndex, e);
				currentIndex++;
			}
		};
	}

	@Override
	public final List<byte[]> subList(final long fromIndex, final long toIndex) {
		final ArrayList<byte[]> result = new ArrayList<byte[]>(Long.valueOf(toIndex - fromIndex).intValue());

		for (long i = fromIndex; i < (toIndex + 1); i++) 
			result.add(get(i));

		return result;
	}
}
