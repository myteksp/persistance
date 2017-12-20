package com.gf.persistance.impl;

import java.nio.MappedByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public final class CollectionsAccessBuffer implements AutoCloseable{
	private static final AtomicLong idGenerator = new AtomicLong(Long.MIN_VALUE);
	private static final long max_id = Long.MAX_VALUE - 1000;
	
	private final long id;
	
	public final MappedByteBuffer buffer;
	public final AtomicLong lastAccess;
	public volatile int size;
	
	public CollectionsAccessBuffer(final MappedByteBuffer buffer, final int size){
		this.size = size;
		this.buffer = buffer;
		this.lastAccess = new AtomicLong(System.currentTimeMillis());
		this.id = idGenerator.getAndIncrement();
		
		if (this.id > max_id){
			synchronized (idGenerator) {
				if (idGenerator.get() > max_id){
					idGenerator.set(Long.MIN_VALUE);
				}
			}
		}
	}
	
	public final void dispose(){}
	
	@Override
	public final String toString() {
		return "CollectionsAccessBuffer [id=" + id + ", buffer=" + buffer + ", lastAccess=" + lastAccess + ", size="
				+ size + "]";
	}

	@Override
	public final int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
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
		final CollectionsAccessBuffer other = (CollectionsAccessBuffer) obj;
		if (id != other.id)
			return false;
		return true;
	}

	@Override
	public final void close() throws Exception {
		try{this.dispose();}catch(final Throwable t){}
	}
}
