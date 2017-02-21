package com.gf.persistance;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.gf.persistance.impl.lists.ByteArrayList;
import com.gf.persistance.impl.lists.ByteArrayLongList;
import com.gf.persistance.impl.lists.Codec;
import com.gf.persistance.impl.lists.FixedSizeByteArrayList;
import com.gf.persistance.impl.lists.FixedSizeByteArrayLongList;
import com.gf.persistance.impl.lists.TypedList;
import com.gf.persistance.impl.lists.TypedLongList;
import com.gf.persistance.impl.maps.ByteArrayLongMap;
import com.gf.persistance.impl.maps.ByteArrayMap;
import com.gf.persistance.impl.maps.TypedLongMap;
import com.gf.persistance.impl.maps.TypedMap;
import com.gf.util.string.JSON;

public final class PersistanceFactory {
	private static final int GC_BALANCE = 1024;
	
	
	public static final <T> PersistedLongList<T> createLongList(final File index, final File store, final Class<T> clz){
		return createLongList(index, store, GC_BALANCE, clz);
	}
	public static final <T> PersistedList<T> createList(final File index, final File store, final Class<T> clz){
		return createList(index, store, GC_BALANCE, clz);
	}
	public static final <T> PersistedLongList<T> createFixedLongList(final File index, final int size, final int gcBallance, final Class<T> clz){
		return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, Integer.BYTES + size, gcBallance), new FixedCodec<T>(size, getCodec(clz)));
	}
	public static final <T> PersistedList<T> createFixedList(final File index, final int size, final int gcBallance, final Class<T> clz){
		return new TypedList<T>(new FixedSizeByteArrayList(index, Integer.BYTES + size, gcBallance), new FixedCodec<T>(size, getCodec(clz)));
	}
	public static final <T> PersistedLongList<T> createLongList(final File index, final File store, final int gcBallance, final Class<T> clz){
		if (clz == Boolean.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, Long.BYTES), getCodec(clz));
		}else if (clz == Long.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, Float.BYTES), getCodec(clz));
		}else if (clz == Short.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, Short.BYTES), getCodec(clz));
		}else if (clz == Float.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, Float.BYTES), getCodec(clz));
		}else if (clz == Integer.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, Integer.BYTES), getCodec(clz));
		}else if (clz == Double.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, Double.BYTES), getCodec(clz));
		}else if (clz == Byte.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, 1), getCodec(clz));
		}else if (clz == Character.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, Character.BYTES), getCodec(clz));
		}else if (clz == boolean.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, 1), getCodec(clz));
		}else if (clz == long.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, Long.BYTES), getCodec(clz));
		}else if (clz == short.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, Short.BYTES), getCodec(clz));
		}else if (clz == float.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, Float.BYTES), getCodec(clz));
		}else if (clz == int.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, Integer.BYTES), getCodec(clz));
		}else if (clz == double.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, Double.BYTES), getCodec(clz));
		}else if (clz == byte.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, 1), getCodec(clz));
		}else if (clz == char.class){
			return new TypedLongList<T>(new FixedSizeByteArrayLongList(index, gcBallance, Character.BYTES), getCodec(clz));
		}else{
			return new TypedLongList<T>(new ByteArrayLongList(index, store, gcBallance), getCodec(clz));
		}
	}
	public static final <T> PersistedList<T> createList(final File index, final File store, final int gcBallance, final Class<T> clz){
		if (clz == Boolean.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, Long.BYTES), getCodec(clz));
		}else if (clz == Long.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, Float.BYTES), getCodec(clz));
		}else if (clz == Short.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, Short.BYTES), getCodec(clz));
		}else if (clz == Float.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, Float.BYTES), getCodec(clz));
		}else if (clz == Integer.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, Integer.BYTES), getCodec(clz));
		}else if (clz == Double.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, Double.BYTES), getCodec(clz));
		}else if (clz == Byte.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, 1), getCodec(clz));
		}else if (clz == Character.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, Character.BYTES), getCodec(clz));
		}else if (clz == boolean.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, 1), getCodec(clz));
		}else if (clz == long.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, Long.BYTES), getCodec(clz));
		}else if (clz == short.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, Short.BYTES), getCodec(clz));
		}else if (clz == float.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, Float.BYTES), getCodec(clz));
		}else if (clz == int.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, Integer.BYTES), getCodec(clz));
		}else if (clz == double.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, Double.BYTES), getCodec(clz));
		}else if (clz == byte.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, 1), getCodec(clz));
		}else if (clz == char.class){
			return new TypedList<T>(new FixedSizeByteArrayList(index, gcBallance, Character.BYTES), getCodec(clz));
		}else{
			return new TypedList<T>(new ByteArrayList(index, store, gcBallance), getCodec(clz));
		}
	}

	public static final <K,V> PersistedMap<K, V> createMap(final File index_file, final File storage_file, 
			final int gcBallance, final long capacity, 
			final Class<K> keyClz, final Class<V> valClz){
		return new TypedMap<K, V>(new ByteArrayMap(index_file, storage_file, gcBallance, capacity), getCodec(keyClz), getCodec(valClz));
	}
	
	
	public static final <K,V> PersistedLongMap<K, V> createLongMap(final File index_file, final File storage_file, 
			final int gcBallance, final long capacity, 
			final Class<K> keyClz, final Class<V> valClz){
		return new TypedLongMap<K, V>(new ByteArrayLongMap(index_file, storage_file, gcBallance, capacity), getCodec(keyClz), getCodec(valClz));
	}
	
	

	@SuppressWarnings("unchecked")
	private static final <E> Codec<E> getCodec(final Class<E> clz){
		if (clz == String.class){
			return (Codec<E>) stringCodec;
		}else if (clz == byte.class || clz == Byte.class){
			return (Codec<E>) byteCodec;
		}else if (clz == boolean.class || clz == Boolean.class){
			return (Codec<E>) boolCodec;
		}else if (clz == int.class || clz == Integer.class){
			return (Codec<E>) intCodec;
		}else if (clz == char.class || clz == Character.class){
			return (Codec<E>) charCodec;
		}else if (clz == double.class || clz == Double.class){
			return (Codec<E>) doubleCodec;
		}else if (clz == float.class || clz == Float.class){
			return (Codec<E>) floatCodec;
		}else if (clz == long.class || clz == Long.class){
			return (Codec<E>) longCodec;
		}else if (clz == short.class || clz == Short.class){
			return (Codec<E>) shortCodec;
		}
		return new Codec<E>() {
			@Override
			public final E decode(final byte[] bytes) {
				if (bytes == null)
					return null;
				
				return JSON.fromJson(new String(bytes), clz);
			}
			@Override
			public final byte[] encode(final E object) {
				if (object == null)
					return null;
				
				return JSON.toJson(object).getBytes();
			}
		};
	}
	private static final Codec<Short> shortCodec = new Codec<Short>() {
		@Override
		public final Short decode(final byte[] bytes) {
			if (bytes == null)
				return null;
			final ByteBuffer wrapped = ByteBuffer.wrap(bytes);
			return wrapped.getShort();
		}
		@Override
		public final byte[] encode(final Short object) {
			if (object == null)
				return null;
			
			final ByteBuffer dbuf = ByteBuffer.allocate(Long.BYTES);
			dbuf.putShort(object);
			return dbuf.array();
		}
	};
	private static final Codec<Long> longCodec = new Codec<Long>() {
		@Override
		public final Long decode(final byte[] bytes) {
			if (bytes == null)
				return null;
			final ByteBuffer wrapped = ByteBuffer.wrap(bytes);
			return wrapped.getLong();
		}
		@Override
		public final byte[] encode(final Long object) {
			if (object == null)
				return null;
			
			final ByteBuffer dbuf = ByteBuffer.allocate(Long.BYTES);
			dbuf.putLong(object);
			return dbuf.array();
		}
	};
	private static final Codec<Float> floatCodec = new Codec<Float>() {
		@Override
		public final Float decode(final byte[] bytes) {
			if (bytes == null)
				return null;
			final ByteBuffer wrapped = ByteBuffer.wrap(bytes);
			return wrapped.getFloat();
		}
		@Override
		public final byte[] encode(final Float object) {
			if (object == null)
				return null;
			final ByteBuffer dbuf = ByteBuffer.allocate(Float.BYTES);
			dbuf.putFloat(object);
			return dbuf.array();
		}
	};
	private static final Codec<Character> charCodec = new Codec<Character>() {
		@Override
		public final Character decode(final byte[] bytes) {
			if (bytes == null)
				return null;
			final ByteBuffer wrapped = ByteBuffer.wrap(bytes);
			return wrapped.getChar();
		}
		@Override
		public final byte[] encode(final Character object) {
			if (object == null)
				return null;
			final ByteBuffer dbuf = ByteBuffer.allocate(Character.BYTES);
			dbuf.putChar(object);
			return dbuf.array();
		}
	};
	private static final Codec<Double> doubleCodec = new Codec<Double>() {
		@Override
		public final Double decode(final byte[] bytes) {
			if (bytes == null)
				return null;
			final ByteBuffer wrapped = ByteBuffer.wrap(bytes);
			return wrapped.getDouble();
		}
		@Override
		public final byte[] encode(final Double object) {
			if (object == null)
				return null;
			final ByteBuffer dbuf = ByteBuffer.allocate(Double.BYTES);
			dbuf.putDouble(object);
			return dbuf.array();
		}
	};
	private static final Codec<String> stringCodec = new Codec<String>() {
		@Override
		public final String decode(final byte[] bytes) {
			if (bytes == null)
				return null;
			try {
				return new String(bytes, "UTF-8");
			} catch (final UnsupportedEncodingException e) {
				return new String(bytes);
			}
		}
		@Override
		public final byte[] encode(final String object) {
			if (object == null)
				return null;
			try {
				return object.getBytes("UTF-8");
			} catch (final UnsupportedEncodingException e) {
				return object.getBytes();
			}
		}
	};
	private static final Codec<Integer> intCodec = new Codec<Integer>() {
		@Override
		public final Integer decode(final byte[] bytes) {
			if (bytes == null)
				return null;
			final ByteBuffer wrapped = ByteBuffer.wrap(bytes);
			return wrapped.getInt();
		}
		@Override
		public final byte[] encode(final Integer object) {
			if (object == null)
				return null;
			final ByteBuffer dbuf = ByteBuffer.allocate(Integer.BYTES);
			dbuf.putInt(object);
			return dbuf.array();
		}
	};
	private static final Codec<Boolean> boolCodec = new Codec<Boolean>() {
		@Override
		public final Boolean decode(final byte[] bytes) {
			if (bytes == null)
				return null;
			
			switch(bytes[0]){
			case Byte.MAX_VALUE:
				return true;
			case Byte.MIN_VALUE:
				return false;
			default:
				throw new RuntimeException("Illigal boolean code: " + bytes[0]);
			}
		}
		@Override
		public final byte[] encode(final Boolean object) {
			if (object == null)
				return null;
			return new byte[]{object?Byte.MAX_VALUE:Byte.MIN_VALUE};
		}
	};
	private static final Codec<Byte> byteCodec = new Codec<Byte>() {
		@Override
		public final Byte decode(final byte[] bytes) {
			if (bytes == null)
				return null;
			
			return bytes[0];
		}
		@Override
		public final byte[] encode(final Byte object) {
			if (object == null)
				return null;
			
			return new byte[]{object};
		}
	};
	
	
	
	
	/*
	 * Codec for general objects with fixed length
	 */
	private static final class FixedCodec<T> implements Codec<T>{
		private final int size;
		private final Codec<T> codec;
		public FixedCodec(final int size, final Codec<T> codec){
			this.size = size;
			this.codec = codec;
		}
		
		@Override
		public final T decode(final byte[] bytes) {
			if (bytes == null)
				return null;
			
			final ByteBuffer buf = ByteBuffer.wrap(bytes);
			buf.position(0);
			final int len = buf.getInt();
			final byte[] res = new byte[len];
			buf.get(res, 0, len);
			return codec.decode(res);
		}
		
		@Override
		public final byte[] encode(final T object) {
			if (object == null)
				return null;
			
			final byte[] res = codec.encode(object);
			final int len = res.length;
			if (len > size)
				throw new RuntimeException("Trying to encode object with lenth of " + len + " bytes into a fixed size collection supporting objects up to " + size + " bytes length.");
			
			final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + len);
			buf.position(0);
			buf.putInt(len);
			buf.put(res);
			
			return buf.array();
		}
	}
}
