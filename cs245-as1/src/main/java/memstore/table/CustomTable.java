package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.TreeMap;

/**
 * Custom table implementation to adapt to provided query mix.
 */
public class CustomTable implements Table {

    protected int numRows;
    protected int numCols;
    protected ByteBuffer columns;
    protected TreeMap<Integer, IntArrayList> singleColIndex0;
    protected TreeMap<Integer, IntArrayList> singleColIndex1;
    protected TreeMap<Integer, IntArrayList> singleColIndex2;
    protected TreeMap<Integer, TreeMap<Integer, Integer>> multiColIndex;
    protected ByteBuffer rowSums;
    protected ByteBuffer preCookedUpdatedRowSums;
    protected BitSet isUpdated;

    public CustomTable() {
        singleColIndex0 = new TreeMap<>();
        singleColIndex1 = new TreeMap<>();
        singleColIndex2 = new TreeMap<>();
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
        this.columns = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        this.rowSums = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows);
        this.preCookedUpdatedRowSums = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows);
        this.isUpdated = new BitSet(numRows);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            int curRowSum = 0;
            for (int colId = 0; colId < numCols; colId++) {
                int curVal = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                putIntField(rowId, colId, curVal);
                curRowSum += curVal;

                // set up single-column index on column-0
                if (colId == 0) {
                    updateSingleColIndex(rowId, curVal);
                }

                // set up index on multiple columns, column-2 goes first, then column-1
                if (colId == 2) {
                    updateMultiColIndex(rowId, curVal);
                }
            }
            preCookRowSum(rowId, curRowSum);    // pre cook the row-sum
        }
    }

    /**
     * Update the single-column index on column-0.
     * @param curVal current value located at (rowId, 0)
     * @param rowId row id of the current value
     */
    private void updateSingleColIndex(int rowId, int curVal) {
        IntArrayList rowIds = singleColIndex0.getOrDefault(curVal, null);
        if (rowIds == null) {
            rowIds = new IntArrayList();
            singleColIndex0.put(curVal, rowIds);
        }
        rowIds.add(rowId);
    }

    /**
     * Update the multi-column index on column-2 & column-1
     * @param curVal current value located at (rowId, 2)
     * @param rowId row id
     */
    private void updateMultiColIndex(int rowId, int curVal) {
        int val1 = getIntField(rowId, 1);
        TreeMap<Integer, Integer> index = multiColIndex.getOrDefault(curVal, null);
        if (index == null) {
            index = new TreeMap<>();
            index.put(val1, getIntField(rowId, 0));
        } else {
            Integer valSum = index.getOrDefault(val1, null);
            if (valSum == null) {
                index.put(val1, 0);
                valSum = 0;
            }
            valSum += getIntField(rowId, 0);
            index.put(val1, valSum);
        }
        multiColIndex.put(curVal, index);
    }

    /**
     * Pre cook the row-sum and the updated row-sum (after val3 = val1 + val2, the original row-sum changes).
     * @param rowId row id
     * @param curRowSum the original row-sum
     */
    private void preCookRowSum(int rowId, int curRowSum) {
        int rowSumOffset = rowId * ByteFormat.FIELD_LEN;
        rowSums.putInt(rowSumOffset, curRowSum);
        int val1 = getIntField(rowId, 1);
        int val2 = getIntField(rowId, 2);
        int val3 = getIntField(rowId, 3);
        preCookedUpdatedRowSums.putInt(rowSumOffset, curRowSum - val3 + val1 + val2);
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
        for (Integer key : singleColIndex0.keySet()) {
            sum += key * singleColIndex0.get(key).size();
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
        TreeMap<Integer, Integer> index = null;
        for (Integer key2 : multiColIndex.keySet()) {
            if (key2 < threshold2) {
                index = multiColIndex.get(key2);
                for (Integer key1 : index.descendingKeySet()) {
                    if (key1 > threshold1) {
                        sum += index.get(key1);
                    } else {
                        break;
                    }
                }
            } else {
                break;
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
        IntArrayList validRowIds = new IntArrayList();
        for (Integer key : singleColIndex0.descendingKeySet()) {
            if (key > threshold) {
                validRowIds.addAll(singleColIndex0.get(key));
            } else {
                break;
            }
        }

        // utilize the pre-cooked rowSum
        for (Integer rowId : validRowIds) {
            int rowSumOffset = ByteFormat.FIELD_LEN * rowId;
            if (isUpdated.get(rowId)) {
                sum += preCookedUpdatedRowSums.getInt(rowSumOffset);
            } else {
                sum += rowSums.getInt(rowSumOffset);
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
        IntArrayList validRowIds = new IntArrayList();
        for (Integer key : singleColIndex0.keySet()) {
            if (key < threshold) {
                validRowIds.addAll(singleColIndex0.get(key));
            } else {
                break;
            }
        }
        for (Integer rowId : validRowIds) {
            isUpdated.set(rowId, true);
        }
        return validRowIds.size();
    }

}
