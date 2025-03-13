package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {
    private String yearGenre;
    private int count;

    public CompositeKey() {}

    public CompositeKey(String yearGenre, int count) {
        this.yearGenre = yearGenre;
        this.count = count;
    }

    public String getYearGenre() {
        return yearGenre;
    }

    public int getCount() {
        return count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(yearGenre);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        yearGenre = in.readUTF();
        count = in.readInt();
    }

    @Override
    public int compareTo(CompositeKey o) {
        int result = yearGenre.compareTo(o.yearGenre);
        if (result == 0) {
            result = Integer.compare(count, o.count);
        }
        return result;
    }

    @Override
    public String toString() {
        return yearGenre + "\t" + count;
    }
}