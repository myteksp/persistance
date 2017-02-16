package com.gf.persistance;

import java.io.File;
import java.io.IOException;



public final class Main {
	public final static void main(final String[] args) throws Exception {
		testInt();
		testBool();
		testByte();
		testChar();
		testDouble();
		testFloat();
		testLong();
		testShort();
		testStrings();


		testLongDouble();
		testLongStrings();
		testLongFixedStrings();
		testFixedStrings();
		
		testMap();
	}
	
	private static final void testMap() throws IOException{
		System.out.println("Test map");
		final File fi = new File("testi.txt");
		final File f = new File("test.txt");
		final PersistedMap<String, String> map = PersistanceFactory.createMap(fi, f, 100, 100, String.class, String.class);
		map.put("key", "value");
		map.put("key", "value 0");
		map.put("key1", "value 1");
		map.put("key2", "value 2");
		map.put("key3", "value 3");
		map.put("key4", "value 4");
		map.put("key5", "value 5");
		
		System.out.println(map.get("key"));
		System.out.println(map.get("key1"));
		System.out.println(map.get("key2"));
		System.out.println(map.get("key3"));
		System.out.println(map.get("key4"));
		System.out.println(map.get("key5"));
		System.out.println(map.size());
		System.out.println("Removed: " + map.remove("key1"));
		System.out.println("Removed: " + map.remove("key3"));
		System.out.println(map.size());
		System.out.println(map.get("key"));
		System.out.println(map.get("key1"));
		System.out.println(map.get("key2"));
		System.out.println(map.get("key3"));
		map.put("key1", "value 1");
		map.put("key3", "value 3");
		
		for(final String key : map.keySet()){
			System.out.println(key + "->" + map.get(key));
		}
		
		System.out.println(map.size());
		
		for (int i = 0; i < 1000; i++) {
			System.out.println(i);
			map.put("key_" + i, "value " + i);
		}
		for(final String key : map.keySet()){
			System.out.println(key + "->" + map.get(key));
		}
		System.out.println(map.size());
		map.delete();
		System.out.println("END===============Test map");
	}
	
	private static final void testFixedStrings() throws Exception{
		System.out.println("Test fixed strings");
		final File fi = new File("testi.txt");
		PersistedList<String> list = PersistanceFactory.createFixedList(fi, 35, 10, String.class);
		for (int i = 0; i < 1000; i++) {
			list.add("Test string " + i);
		}
		long time = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			list.add("Test string " + i);
		}
		for (int i = 0; i < 1000; i++) {
			list.add("Test string " + i);
		}
		time = System.currentTimeMillis() - time;
		for(final String val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());
		list.remove("Test string 3");
		System.out.println("---->" + list.size());
		for(final String val : list){
			System.out.println(val);
		}
		list.remove("Test string 10");
		list.set(1, "newly set!");
		list.set(2, "newly set big string tru la la");
		try{
			list.set(2, "newly set big string tru la lafdsafjsdfgksgdfjjsdfjklhjklsdjfhkjsdhfdsdjkhfds");
			throw new Exception("Shouldn't get here.");
		}catch(final RuntimeException t){
			System.out.println("failed as expected.");
		}
		System.out.println("---->" + list.size());
		for(final String val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.delete();
		System.out.println("END===============Test fixed strings   " + time);
	}

	private static final void testLongFixedStrings() throws Exception{
		System.out.println("Test long fixed strings");
		final File fi = new File("testi.txt");
		PersistedLongList<String> list = PersistanceFactory.createFixedLongList(fi, 35, 10, String.class);
		for (int i = 0; i < 1000; i++) {
			list.add("Test string " + i);
		}
		long time = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			list.add("Test string " + i);
		}
		for (int i = 0; i < 1000; i++) {
			list.add("Test string " + i);
		}
		time = System.currentTimeMillis() - time;
		for(final String val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());
		list.remove("Test string 3");
		System.out.println("---->" + list.size());
		for(final String val : list){
			System.out.println(val);
		}
		list.remove("Test string 10");
		list.set(1, "newly set!");
		list.set(2, "newly set big string tru la la");
		try{
			list.set(2, "newly set big string tru la lafdsafjsdfgksgdfjjsdfjklhjklsdjfhkjsdhfdsdjkhfds");
			throw new Exception("Shouldn't get here.");
		}catch(final RuntimeException t){
			System.out.println("failed as expected.");
		}
		System.out.println("---->" + list.size());
		for(final String val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.delete();
		System.out.println("END===============Test long fixed strings   " + time);
	}

	private static final void testLongStrings() throws IOException{
		System.out.println("Test long strings");
		final File fi = new File("testi.txt");
		final File f = new File("test.txt");
		PersistedLongList<String> list = PersistanceFactory.createLongList(fi, f, String.class);
		for (int i = 0; i < 1000; i++) {
			list.add("Test string " + i);
		}
		long time = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			list.add("Test string " + i);
		}
		for (int i = 0; i < 1000; i++) {
			list.add("Test string " + i);
		}
		time = System.currentTimeMillis() - time;
		for(final String val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());
		list.remove("Test string 3");
		System.out.println("---->" + list.size());
		for(final String val : list){
			System.out.println(val);
		}
		list.remove("Test string 10");
		list.set(1, "newly set!");
		list.set(2, "newly set big string tru la la");
		System.out.println("---->" + list.size());
		for(final String val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.delete();
		System.out.println("END===============Test long strings   " + time);
	}

	private static final void testLongDouble() throws IOException{
		System.out.println("Test long double");
		final File f = new File("test.txt");
		f.createNewFile();
		final PersistedLongList<Double> list = PersistanceFactory.createLongList(f, null, Double.class);
		for (int i = 0; i < 50; i++) {
			list.add(new Double(i).doubleValue());
		}
		for(final double val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.remove((double)20);

		for(final double val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		for (int i = 0; i < 400; i++) {
			list.add((double) i);
		}
		for(final double val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.delete();
		System.out.println("END===============Test long double");
	}



	private static final void testStrings() throws IOException{
		System.out.println("Test strings");
		final File fi = new File("testi.txt");
		final File f = new File("test.txt");
		final PersistedList<String> list = PersistanceFactory.createList(fi, f, String.class);
		for (int i = 0; i < 1000; i++) {
			list.add("Test string " + i);
		}
		long time = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			list.add("Test string " + i);
		}
		for (int i = 0; i < 1000; i++) {
			list.add("Test string " + i);
		}
		time = System.currentTimeMillis() - time;
		for(final String val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());
		list.remove("Test string 3");
		System.out.println("---->" + list.size());
		for(final String val : list){
			System.out.println(val);
		}
		list.remove("Test string 10");
		list.set(1, "newly set!");
		list.set(2, "newly set big string tru la la");
		System.out.println("---->" + list.size());
		for(final String val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.delete();
		System.out.println("END===============Test strings   " + time);
	}


	private static final void testShort() throws IOException{
		System.out.println("Test short");
		final File f = new File("test.txt");
		f.createNewFile();
		final PersistedList<Short> list = PersistanceFactory.createList(f, null, Short.class);
		for (int i = 0; i < 50; i++) {
			list.add((short)i);
		}
		for(final short val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.remove((short)20);

		for(final short val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		for (int i = 0; i < 400; i++) {
			list.add((short) i);
		}
		for(final short val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.delete();
		System.out.println("END===============Test short");
	}


	private static final void testLong() throws IOException{
		System.out.println("Test long");
		final File f = new File("test.txt");
		f.createNewFile();
		final PersistedList<Long> list = PersistanceFactory.createList(f, null, Long.class);
		for (int i = 0; i < 50; i++) {
			list.add(new Long(i).longValue());
		}
		for(final Long val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.remove((long)20);

		for(final Long val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		for (int i = 0; i < 400; i++) {
			list.add((long) i);
		}
		for(final long val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.delete();
		System.out.println("END===============Test long");
	}

	private static final void testFloat() throws IOException{
		System.out.println("Test float");
		final File f = new File("test.txt");
		f.createNewFile();
		final PersistedList<Float> list = PersistanceFactory.createList(f, null, Float.class);
		for (int i = 0; i < 50; i++) {
			list.add(new Float(i).floatValue());
		}
		for(final Float val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.remove((float)20);

		for(final Float val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		for (int i = 0; i < 400; i++) {
			list.add((float) i);
		}
		for(final float val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.delete();
		System.out.println("END===============Test float");
	}

	private static final void testDouble() throws IOException{
		System.out.println("Test double");
		final File f = new File("test.txt");
		f.createNewFile();
		final PersistedList<Double> list = PersistanceFactory.createList(f, null, Double.class);
		for (int i = 0; i < 50; i++) {
			list.add(new Double(i).doubleValue());
		}
		for(final double val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.remove((double)20);

		for(final double val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		for (int i = 0; i < 400; i++) {
			list.add((double) i);
		}
		for(final double val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.delete();
		System.out.println("END===============Test double");
	}

	private static final void testChar() throws IOException{
		System.out.println("Test char");
		final File f = new File("test.txt");
		f.createNewFile();
		final PersistedList<Character> list = PersistanceFactory.createList(f, null, Character.class);
		for (int i = 0; i < 50; i++) {
			list.add((char) i);
		}
		for(final Character val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.remove((Character)(char)20);

		for(final Character val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		for (int i = 0; i < 400; i++) {
			list.add((char) i);
		}
		for(final Character val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.delete();
		System.out.println("END===============Test char");
	}

	private static final void testByte() throws IOException{
		System.out.println("Test byte");
		final File f = new File("test.txt");
		f.createNewFile();
		final PersistedList<Byte> list = PersistanceFactory.createList(f, null, Byte.class);
		for (int i = 0; i < 50; i++) {
			list.add((byte) i);
		}
		for(final Byte val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.remove(20);

		for(final Byte val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		for (int i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
			list.add((byte) i);
		}
		for(final Byte val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.delete();

		final File ff = new File("test.txt");
		ff.createNewFile();
		final PersistedList<Byte> list1 = PersistanceFactory.createList(ff, null, Byte.class);
		for (int i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
			list1.add((byte) i);
		}
		for(final Byte val : list1){
			System.out.println(val);
		}
		System.out.println("---->" + list1.size());
		list1.remove((Byte)(byte)126);
		for(final Byte val : list1){
			System.out.println(val);
		}
		System.out.println("---->" + list1.size());
		list1.delete();
		System.out.println("END===============Test byte");
	}

	private static final void testBool() throws IOException{
		System.out.println("Test boolean");
		final File f = new File("test.txt");
		f.createNewFile();
		final PersistedList<Boolean> list = PersistanceFactory.createList(f, null, Boolean.class);
		for (int i = 0; i < 50; i++) {
			list.add(Math.random() > 0.5);
		}
		for(final Boolean val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.delete();
		System.out.println("END===============Test boolean");
	}

	private static final void testInt() throws IOException{
		System.out.println("Test int");
		final File f = new File("test.txt");
		f.createNewFile();
		final PersistedList<Integer> list = PersistanceFactory.createList(f, null, Integer.class);
		for (int i = 0; i < 50; i++) {
			list.add(i);
		}
		for(final Integer val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.remove(20);

		for(final Integer val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		for (int i = 0; i < 400; i++) {
			list.add(i);
		}
		for(final Integer val : list){
			System.out.println(val);
		}
		System.out.println("---->" + list.size());

		list.delete();
		System.out.println("END===============Test int");
	}
}
