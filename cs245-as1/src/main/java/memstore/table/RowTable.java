package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * RowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 */
public class RowTable implements Table {
    protected int numCols;
    protected int numRows;
    protected ByteBuffer rows;

    public RowTable() { }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                this.rows.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = (rowId * numCols + colId) * ByteFormat.FIELD_LEN;
        return rows.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int offset = (rowId * numCols + colId) * ByteFormat.FIELD_LEN;
        rows.putInt(offset, field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        int offset = 0;
        long sum = 0;
        for (int i = 0;i < numRows;i++) {
            sum += rows.getInt(offset);
            offset += numCols * ByteFormat.FIELD_LEN;
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
        int offset = 0;
        long sum = 0;
        for (int i = 0;i < numRows;i++) {
            int curVal = rows.getInt(offset);
            int val1 = rows.getInt(offset + ByteFormat.FIELD_LEN);
            int val2 = rows.getInt(offset + 2 * ByteFormat.FIELD_LEN);
            if (val1 > threshold1 && val2 < threshold2) {
                sum += curVal;
            }
            offset += numCols * ByteFormat.FIELD_LEN;
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
        int offset = 0;
        long sum = 0;
        for (int i = 0;i < numRows;i++) {
            int curVal = rows.getInt(offset);
            if (curVal > threshold) {
                sum += curVal;
                for (int j = 1;j < numCols;j++) {
                    sum += rows.getInt(offset + j * ByteFormat.FIELD_LEN);
                }
            }
            offset += numCols * ByteFormat.FIELD_LEN;
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
        int offset = 0;
        int updated = 0;
        for (int i = 0;i < numRows;i++) {
            int curVal = rows.getInt(offset);
            if (curVal < threshold) {
                int val1 = rows.getInt(offset + ByteFormat.FIELD_LEN);
                int val2 = rows.getInt(offset + 2 * ByteFormat.FIELD_LEN);
                rows.putInt(offset + 3 * ByteFormat.FIELD_LEN, val1 + val2);
                ++updated;
            }
            offset += numCols * ByteFormat.FIELD_LEN;
        }
        return updated;
    }
}
