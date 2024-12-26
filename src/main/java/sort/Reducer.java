package sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<DoubleWritable, Data, Text, Text> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<Data> values, Context context) throws IOException, InterruptedException {
        for (Data value : values) {
            String formattedValue = String.format("%.2f\t%d", -key.get(), value.getQuantity());
            context.write(new Text(value.getCategory()), new Text(formattedValue));
        }
    }
}
