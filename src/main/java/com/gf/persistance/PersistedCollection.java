package com.gf.persistance;

import java.io.Closeable;
import java.io.File;

public interface PersistedCollection extends Closeable{
	File getIndexFile();
	File getStorageFile();
	void delete();
}
