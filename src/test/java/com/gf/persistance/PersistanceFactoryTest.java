package com.gf.persistance;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

public final class PersistanceFactoryTest {
	@Test
	public final void sanityIntPersistedListsTest() throws IOException{
		final File f = File.createTempFile("testInt", ".tmp");
		f.deleteOnExit();
		final PersistedList<Integer> list = PersistanceFactory.createList(f, null, Integer.class);
		for (int i = 0; i < 50; i++) {
			list.add(i);
		}
		for (int i = 0; i < 50; i++) {
			assertTrue(list.get(i) == i);
		}
		assertTrue(list.size() == 50);
		assertTrue(list.remove(20) == 20);
		assertTrue(list.size() == 49);
		list.add(100);
		assertTrue(list.size() == 50);
		list.delete();
	}
	
	@Test
	public final void sanityStringPersistedListsTest() throws IOException{
		final PersistedLongList<String> list = PersistanceFactory.createLongList(new File("index_file.txt"), new File("store_file.txt"), String.class);
		list.add("first");
		list.add("second");
		list.add("third");
		assertEquals(3, list.size());
		list.remove("second");
		assertEquals(2, list.size());
		assertEquals("third", list.get(1));
		list.clear();
		assertEquals(0, list.size());
		
		for(int i = 0; i < 1000; i++)
			list.add("String_" + i);
		
		assertEquals(1000, list.size());
		list.delete();
	}
	
	@Test
	public final void sanityFixedStringPersistedListsTest() throws IOException{
		final PersistedLongList<String> list = PersistanceFactory.createFixedLongList(new File("FixedStringList.txt"), 35, 10, String.class);
		list.add("first");
		list.add("second");
		list.add("third");
		assertEquals(3, list.size());
		list.remove("second");
		assertEquals("third", list.get(1));
		assertEquals(2, list.size());
		list.clear();
		assertEquals(0, list.size());
		
		for(int i = 0; i < 1000; i++)
			list.add("String_" + i);
		
		assertEquals(1000, list.size());
		list.delete();
	}
	
	@Test
	public final void sanityStringPersistedMapTest() throws IOException{
		final PersistedMap<String, String> map = PersistanceFactory.createMap(new File("testMapIndex.txt"), new File("testMapStore.txt"), 100, 100, String.class, String.class);
		map.put("key", "value");
		map.put("key", "value 0");
		map.put("key1", "value 1");
		map.put("key2", "value 2");
		map.put("key3", "value 3");
		map.put("key4", "value 4");
		map.put("key5", "value 5");
		
		assertEquals(6, map.size());
		
		assertEquals("value 0", map.get("key"));
		assertEquals("value 1", map.get("key1"));
		assertEquals("value 2", map.get("key2"));
		assertEquals("value 3", map.get("key3"));
		assertEquals("value 4", map.get("key4"));
		assertEquals("value 5", map.get("key5"));
		
		assertEquals("value 1", map.remove("key1"));
		assertEquals("value 3", map.remove("key3"));
		
		assertEquals(4, map.size());
		
		assertEquals("value 0", map.get("key"));
		assertEquals("value 2", map.get("key2"));
		assertEquals("value 4", map.get("key4"));
		assertEquals("value 5", map.get("key5"));
		
		map.put("key1", "value 1");
		map.put("key3", "value 3");
		
		assertEquals(6, map.size());
		
		assertEquals("value 0", map.get("key"));
		assertEquals("value 1", map.get("key1"));
		assertEquals("value 2", map.get("key2"));
		assertEquals("value 3", map.get("key3"));
		assertEquals("value 4", map.get("key4"));
		assertEquals("value 5", map.get("key5"));
		
		for (int i = 0; i < 1000; i++) {
			map.put("key_" + i, "value " + i);
		}
		assertEquals(1006, map.size());
		
		map.delete();
	}
}
