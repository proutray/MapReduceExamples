
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IndexPair implements WritableComparable<IndexPair> {

	private IntWritable i;
	public IndexPair(int i) {
		this.i = new IntWritable(i);
	}

	@Override
	public int compareTo(IndexPair o) {
		int cmp = i.compareTo(o.i);
		return -(cmp);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
	}
}
