package hk.ust.comp4651;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PairOfStrings implements WritableComparable<PairOfStrings> {

	private String leftElement;
	private String rightElement;

	/**
	 * Returns the left element
	 * 
	 * @return the left element
	 */
	public String getLeftElement() {
		return leftElement;
	}

	/**
	 * Returns the right element
	 * 
	 * @return the right element
	 */
	public String getRightElement() {
		return rightElement;
	}

	/**
	 * Creates a pair.
	 */
	public PairOfStrings() {
	}

	/**
	 * Creates a pair.
	 * 
	 * @param left
	 *            the left element
	 * @param right
	 *            the right element
	 */
	public PairOfStrings(String left, String right) {
		set(left, right);
	}

	/**
	 * Sets the left and right elements of this pair.
	 * 
	 * @param left
	 * @param right
	 */
	public void set(String left, String right) {
		leftElement = left;
		rightElement = right;
	}

	/*
	 * Deserializes the pair.
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		leftElement = Text.readString(in);
		rightElement = Text.readString(in);
	}

	/*
	 * Serializes the pair.
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, leftElement);
		Text.writeString(out, rightElement);
	}

	/**
	 * Defines a natural sort order for pairs. Pairs are sorted first by the
	 * left element, and then by the right element.
	 *
	 * @return a value less than zero, a value greater than zero, or zero if
	 *         this pair should be sorted before, sorted after, or is equal to
	 *         <code>obj</code>.
	 */
	@Override
	public int compareTo(PairOfStrings pair) {
		String pl = pair.getLeftElement();
		String pr = pair.getRightElement();

		if (leftElement.equals(pl)) {
			return rightElement.compareTo(pr);
		}

		return leftElement.compareTo(pl);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(leftElement, rightElement);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PairOfStrings) {
			PairOfStrings pair = (PairOfStrings) obj;
			return leftElement.equals(pair.getLeftElement()) && rightElement.equals(pair.getRightElement());
		}
			
		return false;
	}
	
	@Override
	public String toString() {
		return leftElement + "\t" + rightElement;
	}
	
	@Override
	public PairOfStrings clone() {
		return new PairOfStrings(leftElement, rightElement);
	}
}
