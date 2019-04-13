package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Custom table implementation to adapt to provided query mix.
 */
public class CustomTable implements Table {

    protected int numRows;
    protected int numCols;
    protected ByteBuffer columns;
    protected TreeMap<Integer, TreeMap<Integer, IntArrayList>> multiColIndex;

    public CustomTable() {
        multiColIndex = new TreeMap<>();
    }

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
        this.columns = ByteBuffer.allocate(ByteFormat.FIELD_LEN*numRows*numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int curVal = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                putIntField(rowId, colId, curVal);

                // set up index on multiple columns, column-2 goes first, then column-1
                if (colId == 2) {
                    int val1 = getIntField(rowId, 1);
                    TreeMap<Integer, IntArrayList> index = multiColIndex.getOrDefault(curVal, null);
                    if (index == null) {
                        index = new TreeMap<>();
                        IntArrayList rowIds = new IntArrayList();
                        rowIds.add(rowId);
                        index.put(val1, rowIds);
                    } else {
                        IntArrayList rowIds = index.getOrDefault(val1, null);
                        if (rowIds == null) {
                            rowIds = new IntArrayList();
                            index.put(val1, rowIds);
                        }
                        rowIds.add(rowId);
                    }
                }
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = (colId * numRows + rowId) * ByteFormat.FIELD_LEN;
        return columns.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int offset = (colId * numRows + rowId) * ByteFormat.FIELD_LEN;
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
        // TODO: Implement this!
        return 0;
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
        // TODO: Implement this!
        return 0;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        // TODO: Implement this!
        return 0;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col1 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        // TODO: Implement this!
        return 0;
    }

}
