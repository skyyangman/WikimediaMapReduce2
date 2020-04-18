import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Comparator;

public class FooAndTitle implements Writable {
    String foo;
    String title;

    public FooAndTitle() {}

    public FooAndTitle(String foo, String title) {
        this.foo = foo;
        this.title = title;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(foo);
        dataOutput.writeUTF(title);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        foo = dataInput.readUTF();
        title = dataInput.readUTF();
    }
}

class Comp implements Comparator<FooAndTitle> {
    public int compare(FooAndTitle o1, FooAndTitle o2) {

        BigInteger a = new BigInteger(o1.foo);
        BigInteger b = new BigInteger(o2.foo);

        if (a.compareTo(b) > 0) {
            return -1;
        } else if (a.compareTo(b) < 0) {
            return 1;
        } else {
            return 0;
        }
    }
}