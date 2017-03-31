import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordComparator extends WritableComparator {

    public WordComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text t1 = (Text) a;
        Text t2 = (Text) b;
        String[] t1Items = t1.toString().split(" ");
        String[] t2Items = t2.toString().split(" ");
        String t1Base = t1Items[0];
        String t2Base = t2Items[1];
        return t1Base.compareTo(t2Base);
    }

}