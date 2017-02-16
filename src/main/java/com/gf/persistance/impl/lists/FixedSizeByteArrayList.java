package com.gf.persistance.impl.lists;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import com.gf.persistance.PersistedList;
import com.gf.persistance.impl.CollectionsAccessBuffer;

public final class FixedSizeByteArrayList implements PersistedList<byte[]>{
	private final int ENTRY_SIZE;

	private final File file;
	private final RandomAccessFile ra_file;
	private final ReentrantLock sizeLocker;
	private final MappedByteBuffer sizeBuffer;
	private final ConcurrentHashMap<Integer, CollectionsAccessBuffer> buffers;
	private final AtomicInteger gc_counter;
	private final int gc_balance;
	private final int gc_lower_balance;
	private final AtomicInteger _size;

	public FixedSizeByteArrayList(final File file, final int entitySize, final int gc_balance){
		if (file == null)
			throw new NullPointerException();

		this.file = file;
		this.sizeLocker = new ReentrantLock(true);
		this.gc_balance = gc_balance;
		this.gc_lower_balance = gc_balance/2;
		this.buffers = new ConcurrentHashMap<Integer, CollectionsAccessBuffer>();
		this.gc_counter = new AtomicInteger(0);
		this.ENTRY_SIZE = entitySize;

		if (!file.exists()){
			try {
				file.createNewFile();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			try {
				this.ra_file = new RandomAccessFile(file, "rw");
				this.sizeBuffer = ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Integer.BYTES);
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
			this._size = new AtomicInteger(0);
			setSize(0);
		}else{
			try {
				this.ra_file = new RandomAccessFile(file, "rw");
				this.sizeBuffer = ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Integer.BYTES);
				this.sizeBuffer.position(0);
				this._size = new AtomicInteger(sizeBuffer.getInt());
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private final void gc_if_needed(){
		if (gc_counter.incrementAndGet() > gc_balance){
			gc_counter.set(0);
			final int toRemove = gc_balance - gc_lower_balance;
			final ArrayList<Integer> keys = new ArrayList<Integer>(buffers.size());

			for(final Integer key : buffers.keySet())
				keys.add(key);

			keys.sort(new Comparator<Integer>() {
				@Override
				public final int compare(final Integer key1, final Integer key2) {
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

	private final CollectionsAccessBuffer getBuffer(final int index){
		try{
			CollectionsAccessBuffer result = buffers.get(index);
			if (result == null){
				final CollectionsAccessBuffer candidate = new CollectionsAccessBuffer(ra_file.getChannel().map(FileChannel.MapMode.READ_WRITE, Integer.BYTES + (index * ENTRY_SIZE), ENTRY_SIZE), ENTRY_SIZE);
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
	public final void close(){
		buffers.clear();
		try {ra_file.close();} catch (final Exception e) {}
	}

	@Override
	public final File getIndexFile(){
		return file;
	}

	@Override
	public File getStorageFile(){
		return null;
	}


	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}

	private final void setSize(final int size){
		_size.set(size);
		sizeLocker.lock();
		try{
			sizeBuffer.position(0);
			sizeBuffer.putInt(size);
		}finally{
			sizeLocker.unlock();
		}
	}

	private final int incrementSize(final int delta){
		final int result = _size.getAndAdd(delta);
		this.setSize(_size.get());
		return result;
	}

	@Override
	public final int size() {
		return _size.get();
	}

	@Override
	public final boolean isEmpty() {
		switch(size()){
		case 0:
			return true;
		default:
			return false;
		}
	}

	@Override
	public final boolean contains(final Object o) {
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
			private final AtomicInteger index = new AtomicInteger(0);
			private volatile byte[] next = null;

			@Override
			public final byte[] next() {
				if (next != null){
					final byte[] res = next;
					next = null;
					index.incrementAndGet();
					return res;
				}
				return get(index.getAndIncrement());
			}

			@Override
			public final boolean hasNext() {
				final int i = index.get();
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
		return this.toArray(new Byte[this.size()]);
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
		final CollectionsAccessBuffer buf = getBuffer(incrementSize(1));
		buf.lock.lock();
		try{
			buf.buffer.position(0);
			buf.buffer.put(e);
		}finally{
			buf.lock.unlock();
		}
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
		setSize(0);
	}

	@Override
	public final byte[] get(final int index) {
		final int size = size();
		if (index < size){
			final CollectionsAccessBuffer buf = getBuffer(index);
			final byte[] res = new byte[ENTRY_SIZE];
			buf.lock.lock();
			try{
				buf.buffer.position(0);
				buf.buffer.get(res);
			}finally{
				buf.lock.unlock();
			}
			return res;
		}
		throw new ArrayIndexOutOfBoundsException("Trying to access index " + index + " in a list of length of " + size);
	}

	@Override
	public final byte[] set(final int index, final byte[] element) {
		final int size = size();
		if (index < size){
			final CollectionsAccessBuffer buf = getBuffer(index);
			final byte[] res = new byte[ENTRY_SIZE];
			buf.lock.lock();
			try{
				buf.buffer.position(0);
				buf.buffer.get(res);
				buf.buffer.position(0);
				buf.buffer.put(element);
				return res;
			}finally{
				buf.lock.unlock();
			}
		}else{
			final int newSize = index + 1;
			setSize(newSize);
			for(int i = size; i < newSize; i++){
				final CollectionsAccessBuffer buf = getBuffer(i);
				buf.lock.lock();
				try{
					buf.buffer.position(0);
					buf.buffer.put(element);
				}finally{
					buf.lock.unlock();
				}
			}
			return null;
		}
	}

	@Override
	public final void add(final int index, final byte[] element) {
		set(index, element);
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
	public final byte[] remove(final int index) {
		final int size = size();
		if (index < size){
			final byte[] result = get(index);
			final int start = index + 1;

			if (start < size)
				for (int i = start; i < size; i++) 
					set(i-1, get(i));

			incrementSize(-1);
			return result;
		}
		return null;
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
			private final AtomicInteger currentIndex = new AtomicInteger(0);
			@Override
			public final void set(final byte[] e) {
				FixedSizeByteArrayList.this.set(currentIndex.get(), e);
			}
			@Override
			public final void remove() {
				FixedSizeByteArrayList.this.remove(currentIndex.get());
			}
			@Override
			public final int previousIndex() {
				return currentIndex.get() - 1;
			}
			@Override
			public final byte[] previous() {
				return FixedSizeByteArrayList.this.get(currentIndex.decrementAndGet());
			}
			@Override
			public final int nextIndex() {
				return currentIndex.get() + 1;
			}
			@Override
			public final byte[] next() {
				return FixedSizeByteArrayList.this.get(currentIndex.incrementAndGet());
			}
			@Override
			public final boolean hasPrevious() {
				return currentIndex.get() > 0;
			}
			@Override
			public final boolean hasNext() {
				return currentIndex.get() < (FixedSizeByteArrayList.this.size() - 1);
			}
			@Override
			public final void add(final byte[] e) {
				FixedSizeByteArrayList.this.add(currentIndex.getAndIncrement(), e);
			}
		};
	}

	@Override
	public final ListIterator<byte[]> listIterator(final int index) {
		return new ListIterator<byte[]>() {
			private final AtomicInteger currentIndex = new AtomicInteger(index);
			@Override
			public final void set(final byte[] e) {
				FixedSizeByteArrayList.this.set(currentIndex.get(), e);
			}
			@Override
			public final void remove() {
				FixedSizeByteArrayList.this.remove(currentIndex.get());
			}
			@Override
			public final int previousIndex() {
				return currentIndex.get() - 1;
			}
			@Override
			public final byte[] previous() {
				return FixedSizeByteArrayList.this.get(currentIndex.decrementAndGet());
			}
			@Override
			public final int nextIndex() {
				return currentIndex.get() + 1;
			}
			@Override
			public final byte[] next() {
				return FixedSizeByteArrayList.this.get(currentIndex.incrementAndGet());
			}
			@Override
			public final boolean hasPrevious() {
				return currentIndex.get() > 0;
			}
			@Override
			public final boolean hasNext() {
				return currentIndex.get() < (FixedSizeByteArrayList.this.size() - 1);
			}
			@Override
			public final void add(final byte[] e) {
				FixedSizeByteArrayList.this.add(currentIndex.getAndIncrement(), e);
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
		try{this.file.delete();}catch(final Throwable t){}
	}
}
