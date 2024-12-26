package sales;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

public class TransactionAnalysis {

    public static class TransactionData implements Writable {
        private double totalAmount;
        private int itemCount;

        public TransactionData() {}

        public TransactionData(double totalAmount, int itemCount) {
            this.totalAmount = totalAmount;
            this.itemCount = itemCount;
        }

        @Override
        public void write(DataOutput output) throws IOException {
            output.writeDouble(totalAmount);
            output.writeInt(itemCount);
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            totalAmount = input.readDouble();
            itemCount = input.readInt();
        }

        public double getTotalAmount() {
            return totalAmount;
        }

        public int getItemCount() {
            return itemCount;
        }

        @Override
        public String toString() {
            return String.format("%.2f\t%d", totalAmount, itemCount);
        }
    }

    public static class TransactionMapper extends Mapper<Object, Text, Text, TransactionData> {
        private Text categoryIdentifier = new Text();

        @Override
        protected void map(Object inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
            String[] recordElements = inputValue.toString().split(",");

            if (recordElements.length == 5 && !recordElements[0].equals("transaction_id")) {
                try {
                    String categoryName = recordElements[2];
                    double unitPrice = Double.parseDouble(recordElements[3]);
                    int itemQuantity = Integer.parseInt(recordElements[4]);

                    categoryIdentifier.set(categoryName);
                    TransactionData transactionDetails = new TransactionData(unitPrice * itemQuantity, itemQuantity);
                    context.write(categoryIdentifier, transactionDetails);
                } catch (NumberFormatException exception) {
                }
            }
        }
    }

    public static class TransactionReducer extends Reducer<Text, TransactionData, Text, Text> {
        @Override
        protected void reduce(Text categoryIdentifier, Iterable<TransactionData> transactions, Context context) throws IOException, InterruptedException {
            double accumulatedAmount = 0.0;
            int accumulatedCount = 0;

            for (TransactionData transaction : transactions) {
                accumulatedAmount += transaction.getTotalAmount();
                accumulatedCount += transaction.getItemCount();
            }
            context.write(categoryIdentifier, new Text(String.format("%.2f\t%d", accumulatedAmount, accumulatedCount)));
        }
    }
}
