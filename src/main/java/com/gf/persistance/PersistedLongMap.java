package com.gf.persistance;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface PersistedLongMap<K,V> extends PersistedCollection{
	long size();   
	boolean isEmpty();
	boolean containsKey(Object key);
	boolean containsValue(Object value);
	V get(Object key);
	V put(K key, V value);
	V remove(Object key);
	void putAll(Map<? extends K, ? extends V> m);
	void clear();
	Set<K> keySet();
	Collection<V> values();
	Set<Map.Entry<K, V>> entrySet();
	default V getOrDefault(Object key, V defaultValue) {
		V v;
		return (((v = get(key)) != null) || containsKey(key)) ? v: defaultValue;
	}
	default void forEach(BiConsumer<? super K, ? super V> action) {
		Objects.requireNonNull(action);
		for (Map.Entry<K, V> entry : entrySet()) {
			K k;
			V v;
			try {
				k = entry.getKey();
				v = entry.getValue();
			} catch(IllegalStateException ise) {
				// this usually means the entry is no longer in the map.
				throw new ConcurrentModificationException(ise);
			}
			action.accept(k, v);
		}
	}
	
	default void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
		Objects.requireNonNull(function);
		for (Map.Entry<K, V> entry : entrySet()) {
			K k;
			V v;
			try {
				k = entry.getKey();
				v = entry.getValue();
			} catch(IllegalStateException ise) {
				throw new ConcurrentModificationException(ise);
			}

			// ise thrown from function is not a cme.
			v = function.apply(k, v);

			try {
				entry.setValue(v);
			} catch(IllegalStateException ise) {
				// this usually means the entry is no longer in the map.
				throw new ConcurrentModificationException(ise);
			}
		}
	}
	
	default V putIfAbsent(K key, V value) {
		V v = get(key);
		if (v == null) {
			v = put(key, value);
		}

		return v;
	}

	
	default boolean replace(K key, V oldValue, V newValue) {
		Object curValue = get(key);
		if (!Objects.equals(curValue, oldValue) ||
				(curValue == null && !containsKey(key))) {
			return false;
		}
		put(key, newValue);
		return true;
	}

	default V replace(K key, V value) {
		V curValue;
		if (((curValue = get(key)) != null) || containsKey(key)) {
			curValue = put(key, value);
		}
		return curValue;
	}

	default V computeIfAbsent(K key,
			Function<? super K, ? extends V> mappingFunction) {
		Objects.requireNonNull(mappingFunction);
		V v;
		if ((v = get(key)) == null) {
			V newValue;
			if ((newValue = mappingFunction.apply(key)) != null) {
				put(key, newValue);
				return newValue;
			}
		}

		return v;
	}

	
	default V computeIfPresent(K key,
			BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		Objects.requireNonNull(remappingFunction);
		V oldValue;
		if ((oldValue = get(key)) != null) {
			V newValue = remappingFunction.apply(key, oldValue);
			if (newValue != null) {
				put(key, newValue);
				return newValue;
			} else {
				remove(key);
				return null;
			}
		} else {
			return null;
		}
	}

	
	default V compute(K key,
			BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		Objects.requireNonNull(remappingFunction);
		V oldValue = get(key);

		V newValue = remappingFunction.apply(key, oldValue);
		if (newValue == null) {
			// delete mapping
			if (oldValue != null || containsKey(key)) {
				// something to remove
				remove(key);
				return null;
			} else {
				// nothing to do. Leave things as they were.
				return null;
			}
		} else {
			// add or replace old mapping
			put(key, newValue);
			return newValue;
		}
	}

	default V merge(K key, V value,
			BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
		Objects.requireNonNull(remappingFunction);
		Objects.requireNonNull(value);
		V oldValue = get(key);
		V newValue = (oldValue == null) ? value :
			remappingFunction.apply(oldValue, value);
		if(newValue == null) {
			remove(key);
		} else {
			put(key, newValue);
		}
		return newValue;
	}
}
