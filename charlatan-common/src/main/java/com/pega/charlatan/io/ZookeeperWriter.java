package com.pega.charlatan.io;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class ZookeeperWriter {

	private DataOutput output;

	public ZookeeperWriter(DataOutput out) {
		this.output = out;
	}

	public <T> void startVector(List<T> v) throws IOException {
		if (v == null) {
			writeInt(-1);
			return;
		}
		writeInt(v.size());
	}

	public void writeInt(int i) throws IOException {
		output.writeInt(i);
	}

	public void writeLong(long l) throws IOException {
		output.writeLong(l);
	}

	public void write(byte[] bytes) throws IOException {
		output.write(bytes);
	}

	public void write(byte[] bytes, int off, int length) throws IOException {
		output.write(bytes, off, length);
	}

	public void writeBoolean(boolean b) throws IOException {
		output.writeBoolean(b);
	}

	public void writeString(String s) throws IOException {
		if (s == null) {
			writeInt(-1);
			return;
		}

		writeInt(s.length());
		write(s.getBytes("UTF8"));
	}

	public void writeBuffer(byte[] b) throws IOException {
		if (b == null) {
			writeInt(-1);
			return;
		}
		writeInt(b.length);
		write(b);
	}

	public void writeVector(List<String> vector) throws IOException {
		if (vector == null) {
			writeInt(-1);
		} else {
			int length = vector.size();
			writeInt(length);

			for (String s : vector) {
				writeString(s);
			}
		}
	}
}