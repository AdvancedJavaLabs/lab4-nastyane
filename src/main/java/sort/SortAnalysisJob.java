package sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortAnalysisJob {

    public static class TransactionRecord implements Writable {
        private String categoryName;
        private int totalQuantity;

        public TransactionRecord() {}

        public TransactionRecord(String categoryName, int totalQuantity) {
            this.categoryName = categoryName;
            this.totalQuantity = totalQuantity;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(categoryName);
            dataOutput.writeInt(totalQuantity);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            categoryName = dataInput.readUTF();
            totalQuantity = dataInput.readInt();
        }

        public String getCategoryName() {
            return categoryName;
        }

        public int getTotalQuantity() {
            return totalQuantity;
        }

        @Override
        public String toString() {
            return categoryName + "\t" + totalQuantity;
        }
    }

    public static class RevenueMapper extends Mapper<Object, Text, DoubleWritable, TransactionRecord> {
        private final DoubleWritable revenueKey = new DoubleWritable();

        @Override
        protected void map(Object inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
            String[] recordFields = inputValue.toString().split("\t");

            if (recordFields.length == 3) {
                try {
                    String category = recordFields[0];
                    double revenue = Double.parseDouble(recordFields[1]);
                    int quantity = Integer.parseInt(recordFields[2]);

                    revenueKey.set(-revenue);
                    context.write(revenueKey, new TransactionRecord(category, quantity));
                } catch (NumberFormatException e) {
                    System.err.println("Invalid number format in input: " + inputValue.toString());
                }
            }
        }
    }

    public static class RevenueReducer extends Reducer<DoubleWritable, TransactionRecord, Text, Text> {

        @Override
        protected void reduce(DoubleWritable revenueKey, Iterable<TransactionRecord> records, Context context) throws IOException, InterruptedException {
            for (TransactionRecord record : records) {
                String formattedValue = String.format("%.2f\t%d", -revenueKey.get(), record.getTotalQuantity());
                context.write(new Text(record.getCategoryName()), new Text(formattedValue));
            }
        }
    }

}
