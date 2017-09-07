package com.pega.zooikeeper.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZookeeperReader {

	private DataInput di;

	public ZookeeperReader(DataInputStream dataInput) {
		this.di = dataInput;
	}

	public String readString() throws IOException {
		int length = di.readInt();

		if (length > -1) {
			byte b[] = new byte[length];
			di.readFully(b);
			return new String(b, "UTF-8");
		}

		return null;
	}

	public long readLong() throws IOException {
		return di.readLong();
	}

	public int readInt() throws IOException {
		return di.readInt();
	}

	public List<String> readVector() throws IOException {
		int vectorLength = readInt();

		if (vectorLength > -1) {
			List<String> vector = new ArrayList<String>();

			for (int i = 0; i < vectorLength; i++) {
				vector.add(readString());
			}

			return vector;
		}

		return null;
	}

	public void readFully(byte[] b) throws IOException {
		di.readFully(b);
	}

	public boolean readBoolean() throws IOException {
		return di.readBoolean();
	}

	public byte[] readBuffer() throws IOException {
		int len = readInt();
		if (len == -1) return null;
		byte[] arr = new byte[len];
		readFully(arr);
		return arr;
	}
}
