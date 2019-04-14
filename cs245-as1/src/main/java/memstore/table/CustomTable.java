package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Custom table implementation to adapt to provided query mix.
 */
public class CustomTable implements Table {

    protected int numRows;
    protected int numCols;
    protected int numHiddenCols;
    protected ByteBuffer columns;
    protected TreeMap<Integer, TreeMap<Integer, IntArrayList>> multiColIndex;
    protected TreeMap<Integer, IntArrayList> singleColIndex;
    protected ByteBuffer rowSums;
    public static int ROW_SUM_FIELD_LEN = 8;

    public CustomTable() {
        numHiddenCols = 1;
        multiColIndex = new TreeMap<>();
        singleColIndex = new TreeMap<>();
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
        this.columns = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        this.rowSums = ByteBuffer.allocate(ROW_SUM_FIELD_LEN * numRows);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            long curRowSum = 0L;
            for (int colId = 0; colId < numCols; colId++) {
                int curVal = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                putIntField(rowId, colId, curVal);
                curRowSum += curVal;

                // set up single-column index on column-0
                if (colId == 0) {
                    IntArrayList rowIds = singleColIndex.getOrDefault(curVal, null);
                    if (rowIds == null) {
                        rowIds = new IntArrayList();
                        singleColIndex.put(curVal, rowIds);
                    }
                    rowIds.add(rowId);
                }

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
                    multiColIndex.put(curVal, index);
                }
            }
            int rowSumOffset = rowId * ROW_SUM_FIELD_LEN;
            rowSums.putLong(rowSumOffset, curRowSum);
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
        for (Integer key : singleColIndex.keySet()) {
            sum += key * singleColIndex.get(key).size();
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
        IntArrayList validRowIds = new IntArrayList();
        for (Integer key2 : multiColIndex.keySet()) {
            if (key2 < threshold2) {
                TreeMap<Integer, IntArrayList> index = multiColIndex.get(key2);
                for (Integer key1 : index.keySet()) {
                    if (key1 > threshold1) {
                        validRowIds.addAll(index.get(key1));
                    }
                }
            } else {
                break;
            }
        }

        for (Integer rowId : validRowIds) {
            sum += getIntField(rowId, 0);
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
        IntArrayList validRowIds = new IntArrayList();
        for (Integer key : singleColIndex.keySet()) {
            if (key > threshold) {
                validRowIds.addAll(singleColIndex.get(key));
            }
        }

        // utilize the pre-cooked rowSum
        for (Integer rowId : validRowIds) {
            int rowSumOffset = ROW_SUM_FIELD_LEN * rowId;
            sum += rowSums.getLong(rowSumOffset);
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
        IntArrayList validRowIds = new IntArrayList();
        for (Integer key : singleColIndex.keySet()) {
            if (key < threshold) {
                validRowIds.addAll(singleColIndex.get(key));
            } else {
                break;
            }
        }

        for (Integer rowId : validRowIds) {
            int val1 = getIntField(rowId, 1);
            int val2 = getIntField(rowId, 2);
            int val3 = getIntField(rowId, 3);
            long diff = val1 + val2 - val3;
            putIntField(rowId, 3, val1 + val2);
            int rowSumOffset = ROW_SUM_FIELD_LEN * rowId;
            long prevRowSum = rowSums.getLong(rowSumOffset);
            rowSums.putLong(rowSumOffset, prevRowSum + diff);
        }
        int updated = validRowIds.size();
        return updated;
    }

}
