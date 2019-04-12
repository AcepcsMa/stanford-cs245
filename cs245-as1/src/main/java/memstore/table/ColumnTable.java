package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * ColumnTable, which stores data in column-major format.
 * That is, data is laid out like
 *   col 1 | col 2 | ... | col m.
 */
public class ColumnTable implements Table {
    int numCols;
    int numRows;
    ByteBuffer columns;

    public ColumnTable() { }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.columns = ByteBuffer.allocate(ByteFormat.FIELD_LEN*numRows*numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
                this.columns.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN*colId));
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        return columns.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        columns.putInt(offset, field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        long sum = 0;
        for (int i = 0;i < numRows;i++) {
            sum += getIntField(i, 0);
        }
        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        long sum = 0;
        for (int i = 0;i < numRows;i++) {
            int val1 = getIntField(i, 1);
            int val2 = getIntField(i, 2);
            if (val1 > threshold1 && val2 < threshold2) {
                sum += getIntField(i, 0);
            }
        }
        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        long sum = 0;
        for (int i = 0;i < numRows;i++) {
            int curVal = getIntField(i, 0);
            if (curVal > threshold) {
                sum += curVal;
                for (int j = 1;j < numCols;j++) {
                    sum += getIntField(i, j);
                }
            }
        }
        return sum;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col1 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        int updated = 0;
        for (int i = 0;i < numRows;i++) {
            int curVal = getIntField(i, 0);
            if (curVal < threshold) {
                int val1 = getIntField(i, 1);
                int val2 = getIntField(i, 2);
                putIntField(i, 3, val1 + val2);
                ++updated;
            }
        }
        return updated;
    }
}
