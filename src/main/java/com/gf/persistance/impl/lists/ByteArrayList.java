package com.gf.persistance.impl.lists;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.gf.persistance.PersistedList;
import com.gf.persistance.impl.CollectionsAccessBuffer;

public final class ByteArrayList implements PersistedList<byte[]>{
	private final TypedList<TripleLong> index_list;
	private final File storage_file;
	private final RandomAccessFile ra_file;
	private final int gc_balance;
	private final int gc_lower_balance;
	private final HashMap<TripleLong, CollectionsAccessBuffer> buffers;
	private final MappedByteBuffer offsetBuffer;

	public ByteArrayList(final File index_file, final File storage_file){
		this(index_file, storage_file, 100);
	}


	private static final Codec<TripleLong> tripleLongCodec = new Codec<TripleLong>() {
		@Override
		public final TripleLong decode(final byte[] bytes) {
			final ByteBuffer wrapped = ByteBuffer.wrap(bytes);
			return new TripleLong(wrapped.getLong(), wrapped.getLong(), wrapped.getLong());
		}
		@Override
		public final byte[] encode(final TripleLong object) {
			final ByteBuffer dbuf = ByteBuffer.allocate(TripleLong.BYTES);
			dbuf.putLong(object.value1);
			dbuf.putLong(object.value2);
			dbuf.putLong(object.value3);
			return dbuf.array();
		}
	};

	public ByteArrayList(final File index_file, final File storage_file, final int gc_balance){
		this.index_list = new TypedList<TripleLong>(new FixedSizeByteArrayList(index_file, TripleLong.BYTES, gc_balance), tripleLongCodec);
		this.storage_file = storage_file;
		this.gc_balance = gc_balance;
		this.gc_lower_balance = gc_balance/2;
		this.buffers = new HashMap<TripleLong, CollectionsAccessBuffer>();

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
				this.offsetBuffer.putLong(Integer.valueOf(Long.BYTES).longValue());
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		}else{
			try {
				this.ra_file = new RandomAccessFile(storage_file, "rw");
				this.offsetBuffer = ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Long.BYTES);
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private final void gc_if_needed(){
		if (buffers.size() > gc_balance){
			final int toRemove = gc_balance - gc_lower_balance;
			final ArrayList<TripleLong> keys = new ArrayList<TripleLong>(buffers.size());

			for(final TripleLong key : buffers.keySet())
				keys.add(key);

			keys.sort(new Comparator<TripleLong>() {
				@Override
				public final int compare(final TripleLong key1, final TripleLong key2) {
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

	private final CollectionsAccessBuffer getBuffer(final TripleLong range){
		try{
			CollectionsAccessBuffer result = buffers.get(range);
			if (result == null){
				final long entrySze = range.value2 - range.value1;
				final CollectionsAccessBuffer candidate = new CollectionsAccessBuffer(ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, range.value1, entrySze), (int) entrySze);
				result = buffers.putIfAbsent(range, candidate);
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
			throw new RuntimeException("Failed to get range: " + range,t);
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
	public final void close() throws IOException {
		try{ra_file.close();}catch(final Throwable t){}
		try{index_list.close();}catch(final Throwable t){}
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
	public final int size() {
		return index_list.size();
	}
	@Override
	public final boolean isEmpty() {
		return index_list.isEmpty();
	}
	@Override
	public final boolean contains(final Object o) {
		if (o instanceof byte[]){
			final byte[] arr = (byte[]) o;
			for(final byte[] e : this)
				if (arrayEquals(arr, e))
					return true;
		}
		return false;
	}

	@Override
	public final Iterator<byte[]> iterator() {
		return new Iterator<byte[]>() {
			private final Iterator<TripleLong> iter = index_list.iterator();
			@Override
			public final byte[] next() {
				final TripleLong range = iter.next();
				final CollectionsAccessBuffer buf = getBuffer(range);
				final byte[] res = new byte[buf.size];
				buf.buffer.position(0);
				buf.buffer.get(res);
				return res;
			}
			@Override
			public final boolean hasNext() {
				return iter.hasNext();
			}
		};
	}

	@Override
	public final Object[] toArray() {
		return this.toArray(new Object[this.size()]);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final <T> T[] toArray(final T[] a) {
		final int size = size();
		final int len = a.length;
		for (int i = 0; i < size; i++) 
			if (i < len)
				a[i] = (T) get(i);
			else
				return a;

		return a;
	}

	@Override
	public final boolean add(final byte[] e) {
		final TripleLong range = alocateNewRange(e.length);
		final CollectionsAccessBuffer buf = getBuffer(range);
		buf.buffer.position(0);
		buf.buffer.put(e);
		index_list.add(range);
		return true;
	}

	@Override
	public final boolean remove(final Object o) {
		if (o == null)
			return false;

		final ArrayList<Integer> indexes = new ArrayList<Integer>();
		if (o instanceof byte[]){
			final byte[] O = (byte[]) o;
			final int len = size();
			for (int i = 0; i < len; i++) {
				final byte[] item = get(i);
				if (arrayEquals(item, O)){
					indexes.add(i);
				}
			}
			for(final int ind : indexes){
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
	public final boolean addAll(final int index, final Collection<? extends byte[]> c) {
		int currentIndex = index;
		final int size = size();
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
		this.offsetBuffer.position(0);
		this.offsetBuffer.putLong(Long.BYTES);
		this.index_list.clear();
	}

	@Override
	public final byte[] get(final int index) {
		final TripleLong range = index_list.get(index);
		final CollectionsAccessBuffer buf = getBuffer(range);
		final byte[] res = new byte[buf.size];
		buf.buffer.position(0);
		buf.buffer.get(res);
		return res;
	}

	private final TripleLong alocateNewRange(final long size){
		final long start;
		final long end;
		offsetBuffer.position(0);
		start = offsetBuffer.getLong();
		end = start + size;
		offsetBuffer.position(0);
		offsetBuffer.putLong(end);
		return new TripleLong(start, end, size);
	}

	@Override
	public final byte[] set(final int index, byte[] element) {
		final TripleLong range = index_list.get(index);
		final CollectionsAccessBuffer buf = getBuffer(range);
		final int oldSize = buf.size;
		final int newSize = element.length;
		final byte[] res = new byte[oldSize];

		buf.buffer.position(0);
		buf.buffer.get(res);
		if (newSize == oldSize){
			//in this case no action need to be performed, since new value is equal in size to the old one.
			buf.buffer.position(0);
			buf.buffer.put(element);
		}else if (newSize < oldSize){
			//in this case new value is smaller than the allocated slot, hence could be placed into an existing slot.
			buf.buffer.position(0);
			buf.buffer.put(element);
			buf.size = newSize;
			//the only thing to do is to replace the ranges in the index list.
			final TripleLong new_range = new TripleLong(range.value1, range.value1 + newSize, range.value3);
			index_list.set(index, new_range);
			buffers.remove(range);
		}else{
			//in this case value is greater than the previous value, but it can still be smaller than originally allocated space.
			if (newSize <= range.value3){
				//in this case than value is smaller than the originally allocated space, and so, this case is identical to the (newSize < oldSize)
				buf.buffer.position(0);
				buf.buffer.put(element);
				buf.size = newSize;
				final TripleLong new_range = new TripleLong(range.value1, range.value1 + newSize, range.value3);
				index_list.set(index, new_range);
				buffers.remove(range);
			}else{
				//in this case the new value is greater then the previous and the originally allocated. 
				//the only option is to allocate a new space and to replace the region in the index.
				final TripleLong new_range = alocateNewRange(newSize);
				final CollectionsAccessBuffer new_buf = getBuffer(new_range);

				new_buf.buffer.position(0);
				new_buf.buffer.put(element);

				index_list.set(index, new_range);
				buffers.remove(range);
				buf.dispose();
			}
		}
		return res;
	}

	@Override
	public final void add(final int index, final byte[] element) {
		set(index, element);
	}

	@Override
	public final byte[] remove(final int index) {
		final TripleLong range = index_list.remove(index);
		if (range != null){
			final CollectionsAccessBuffer buf = buffers.remove(range);
			if (buf != null){
				final byte[] res = new byte[buf.size];
				buf.buffer.position(0);
				buf.buffer.get(res);
				return res;
			}
		}
		return null;
	}

	@Override
	public final int indexOf(final Object o) {
		if (o instanceof byte[]){
			final byte[] O = (byte[]) o;
			final int len = size();
			for (int i = 0; i < len; i++) 
				if (arrayEquals(get(i), O))
					return i;
		}
		return -1;
	}

	@Override
	public final int lastIndexOf(final Object o) {
		if (o instanceof byte[]){
			final byte[] O = (byte[]) o;

			for(int i = size() - 1; i > -1; i--)
				if (arrayEquals(get(i), O))
					return i;

		}
		return -1;
	}

	@Override
	public final ListIterator<byte[]> listIterator() {
		return new ListIterator<byte[]>() {
			private volatile int currentIndex = 0;
			@Override
			public final void set(final byte[] e) {
				ByteArrayList.this.set(currentIndex, e);
			}
			@Override
			public final void remove() {
				ByteArrayList.this.remove(currentIndex);
			}
			@Override
			public final int previousIndex() {
				return currentIndex - 1;
			}
			@Override
			public final byte[] previous() {
				currentIndex--;
				return ByteArrayList.this.get(currentIndex);
			}
			@Override
			public final int nextIndex() {
				return currentIndex + 1;
			}
			@Override
			public final byte[] next() {
				currentIndex++;
				return ByteArrayList.this.get(currentIndex);
			}
			@Override
			public final boolean hasPrevious() {
				return currentIndex > 0;
			}
			@Override
			public final boolean hasNext() {
				return currentIndex < (ByteArrayList.this.size() - 1);
			}
			@Override
			public final void add(final byte[] e) {
				ByteArrayList.this.add(currentIndex, e);
				currentIndex++;
			}
		};
	}

	@Override
	public final ListIterator<byte[]> listIterator(final int index) {
		return new ListIterator<byte[]>() {
			private volatile int currentIndex = index;
			@Override
			public final void set(final byte[] e) {
				ByteArrayList.this.set(currentIndex, e);
			}
			@Override
			public final void remove() {
				ByteArrayList.this.remove(currentIndex);
			}
			@Override
			public final int previousIndex() {
				return currentIndex - 1;
			}
			@Override
			public final byte[] previous() {
				currentIndex--;
				return ByteArrayList.this.get(currentIndex);
			}
			@Override
			public final int nextIndex() {
				return currentIndex + 1;
			}
			@Override
			public final byte[] next() {
				currentIndex++;
				return ByteArrayList.this.get(currentIndex);
			}
			@Override
			public final boolean hasPrevious() {
				return currentIndex > 0;
			}
			@Override
			public final boolean hasNext() {
				return currentIndex < (ByteArrayList.this.size() - 1);
			}
			@Override
			public final void add(final byte[] e) {
				ByteArrayList.this.add(currentIndex, e);
				currentIndex++;
			}
		};
	}

	@Override
	public final List<byte[]> subList(final int fromIndex, final int toIndex) {
		final ArrayList<byte[]> result = new ArrayList<byte[]>(toIndex - fromIndex);

		for (int i = fromIndex; i < (toIndex + 1); i++) 
			result.add(get(i));

		return result;
	}

	@Override
	public synchronized final void delete() {
		try{this.clear();}catch(final Throwable t){}
		try{this.close();}catch(final Throwable t){}
		try{this.index_list.delete();}catch(final Throwable t){}
		try{this.storage_file.delete();}catch(final Throwable t){}
	}




	private static final class TripleLong{
		public static final int BYTES = Long.BYTES + Long.BYTES + Long.BYTES;

		public final long value1;
		public final long value2;
		public final long value3;

		public TripleLong(final long value1, final long value2, final long value3){
			this.value1 = value1;
			this.value2 = value2;
			this.value3 = value3;
		}
		@Override
		public final String toString() {
			return "DoubleLong [value1=" + value1 + ", value2=" + value2 + ", value3=" + value3 + "]";
		}
		@Override
		public final int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (value1 ^ (value1 >>> 32));
			result = prime * result + (int) (value2 ^ (value2 >>> 32));
			result = prime * result + (int) (value3 ^ (value3 >>> 32));
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
			final TripleLong other = (TripleLong) obj;
			if (value1 != other.value1)
				return false;
			if (value2 != other.value2)
				return false;
			if (value3 != other.value3)
				return false;
			return true;
		}
	}
}
