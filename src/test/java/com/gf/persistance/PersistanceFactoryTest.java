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
	
}
