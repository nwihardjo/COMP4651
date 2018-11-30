/**
 * 
 */
package hk.ust.comp4651;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

/**
 * @author Wei Wang
 *
 */
public class HashMapStringIntWritable extends HashMap<String, Integer>
		implements Writable {
	/**
	 * The serialization runtime associates with each serializable class a
	 * version number, called a serialVersionUID, which is used during
	 * deserialization to verify that the sender and receiver of a serialized
	 * object have loaded classes for that object that are compatible with
	 * respect to serialization.
	 */
	private static final long serialVersionUID = -5222591318213533456L;

	/*
	 * Creates StripeWritable
	 */
	public HashMapStringIntWritable() {
		super();
	}

	/*
	 * Deserializes HashMapStringIntWritable
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.clear();

		int numEntries = in.readInt();
		if (numEntries == 0) {
			return;
		}

		for (int i = 0; i < numEntries; i++) {
			String key = in.readUTF();
			int value = in.readInt();
			this.put(key, value);
		}
	}

	/*
	 * Serializes HashMapStringIntWritable
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.size());
		if (this.size() == 0) {
			return;
		}

		for (Map.Entry<String, Integer> e : this.entrySet()) {
			out.writeUTF(e.getKey());
			out.writeInt(e.getValue());
		}
	}

	/**
	 * Increments the value of the key by one
	 * 
	 * @param key
	 */
	public void increment(String key) {
		if (this.containsKey(key)) {
			this.put(key, this.get(key) + 1);
		} else {
			this.put(key, 1);
		}
	}

	/**
	 * Increments the value of the key by inc
	 * 
	 * @param key
	 * @param inc
	 *            is the increments by which the value should be incremented
	 */
	public void increment(String key, int inc) {
		if (this.containsKey(key)) {
			this.put(key, this.get(key) + inc);
		} else {
			this.put(key, inc);
		}
	}

	/**
	 * Adds up two HashMapStringIntWritable
	 * 
	 * @param that
	 */
	public void plus(HashMapStringIntWritable that) {
		for (Map.Entry<String, Integer> e : that.entrySet()) {
			String key = e.getKey();
			this.increment(key, e.getValue());
		}
	}

}
