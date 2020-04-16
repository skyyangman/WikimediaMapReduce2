import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Data implements Writable {
    String date;
    String pageView;

    public Data() {}

    public Data(String date, String pageView) {
        this.date = date;
        this.pageView = pageView;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(date);
        dataOutput.writeUTF(pageView);
    }

    public void readFields(DataInput dataInput) throws IOException {
        date = dataInput.readUTF();
        pageView = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return date + pageView;
    }
}
