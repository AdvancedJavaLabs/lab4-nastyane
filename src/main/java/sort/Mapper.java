package sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, DoubleWritable, Data> {
    private final DoubleWritable outKey = new DoubleWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        if (fields.length == 3) {
            try {
                String categoryKey = fields[0];
                double val = Double.parseDouble(fields[1]);
                int quantity = Integer.parseInt(fields[2]);
                outKey.set(-val);
                context.write(outKey, new Data(categoryKey, quantity));
            } catch (NumberFormatException e) {
                System.err.println("Invalid number format in input: " + value.toString());
            }
        }
    }
}
