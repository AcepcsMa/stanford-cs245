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
    protected int numHiddenCols;
    protected ByteBuffer columns;
    protected TreeMap<Integer, IntArrayList> singleColIndex0;
    protected TreeMap<Integer, IntArrayList> singleColIndex1;
    protected TreeMap<Integer, IntArrayList> singleColIndex2;
    protected ByteBuffer rowSums;
    protected ByteBuffer preCookedCol3;
    protected BitSet isUpdated;
    public static int ROW_SUM_FIELD_LEN = 8;

    public CustomTable() {
        numHiddenCols = 1;
        singleColIndex0 = new TreeMap<>();
        singleColIndex1 = new TreeMap<>();
        singleColIndex2 = new TreeMap<>();
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
        this.preCookedCol3 = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows);
        this.isUpdated = new BitSet(numRows);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            long curRowSum = 0L;
            for (int colId = 0; colId < numCols; colId++) {
                int curVal = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                putIntField(rowId, colId, curVal);
                curRowSum += curVal;

                // set up single-column index on column-0
                if (colId == 0) {
                    IntArrayList rowIds = singleColIndex0.getOrDefault(curVal, null);
                    if (rowIds == null) {
                        rowIds = new IntArrayList();
                        singleColIndex0.put(curVal, rowIds);
                    }
                    rowIds.add(rowId);
                }

                // set up index on multiple columns, column-2 goes first, then column-1
                if (colId == 1 || colId == 2) {
                    TreeMap<Integer, IntArrayList> curIndex = colId == 1 ? singleColIndex1 : singleColIndex2;
                    IntArrayList rowIds = curIndex.getOrDefault(curVal, null);
                    if (rowIds == null) {
                        rowIds = new IntArrayList();
                        curIndex.put(curVal, rowIds);
                    }
                    rowIds.add(rowId);
                }

                // pre cook col3 = col1 + col2
                if (colId == 3) {
                    int val1 = getIntField(rowId, 1);
                    int val2 = getIntField(rowId, 2);
                    int offset = rowId * ByteFormat.FIELD_LEN;
                    preCookedCol3.putInt(offset, val1 + val2);
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
//        long start = System.currentTimeMillis();
        long sum = 0;
        for (Integer key : singleColIndex0.keySet()) {
            sum += key * singleColIndex0.get(key).size();
        }
//        long end = System.currentTimeMillis();
//        System.out.println(String.format("columnSum: %d ms", end - start));
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
//        long start = System.currentTimeMillis();
        long sum = 0;
        IntArrayList validRowIds2 = new IntArrayList();
        for (Integer key2 : singleColIndex2.keySet()) {
            if (key2 < threshold2) {
                validRowIds2.addAll(singleColIndex2.get(key2));
            } else {
                break;
            }
        }

        for (Integer rowId : validRowIds2) {
            int val1 = getIntField(rowId, 1);
            if (val1 > threshold1) {
                sum += getIntField(rowId, 0);
            }
        }
//        long end = System.currentTimeMillis();
//        System.out.println(String.format("predicatedColumnSum: %d ms", end - start));
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
//        long start = System.currentTimeMillis();
        long sum = 0;
        IntArrayList validRowIds = new IntArrayList();
        for (Integer key : singleColIndex0.keySet()) {
            if (key > threshold) {
                validRowIds.addAll(singleColIndex0.get(key));
            }
        }

        // utilize the pre-cooked rowSum
        for (Integer rowId : validRowIds) {
            int rowSumOffset = ROW_SUM_FIELD_LEN * rowId;
            sum += rowSums.getLong(rowSumOffset);
            if (isUpdated.get(rowId)) {
                sum -= getIntField(rowId, 3);
                int preCookedOffset = rowId * ByteFormat.FIELD_LEN;
                sum += preCookedCol3.getInt(preCookedOffset);
            }
        }
//        long end = System.currentTimeMillis();
//        System.out.println(String.format("predicatedAllColumnsSum: %d ms", end - start));
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
//        long start = System.currentTimeMillis();
        IntArrayList validRowIds = new IntArrayList();
        for (Integer key : singleColIndex0.keySet()) {
            if (key < threshold) {
                validRowIds.addAll(singleColIndex0.get(key));
            } else {
                break;
            }
        }
//        long end = System.currentTimeMillis();
//        System.out.println(String.format("PredicatedUpdate Phase #1: %d ms", end - start));

//        start = System.currentTimeMillis();
        for (Integer rowId : validRowIds) {
            isUpdated.set(rowId, true);
        }
        return validRowIds.size();
//        end = System.currentTimeMillis();
//        System.out.println(String.format("PredicatedUpdate Phase #2 (BitSet): %d ms", end - start));

//        start = System.currentTimeMillis();
//        for (Integer rowId : validRowIds) {
//            int val1 = getIntField(rowId, 1);
//            int val2 = getIntField(rowId, 2);
//            int val3 = getIntField(rowId, 3);
//            long diff = val1 + val2 - val3;
//            putIntField(rowId, 3, val1 + val2);
//            int rowSumOffset = ROW_SUM_FIELD_LEN * rowId;
//            long prevRowSum = rowSums.getLong(rowSumOffset);
//            rowSums.putLong(rowSumOffset, prevRowSum + diff);
//        }
//        int updated = validRowIds.size();
//        end = System.currentTimeMillis();
//        System.out.println(String.format("Valid Row Count: %d", updated));
//        System.out.println(String.format("PredicatedUpdate Phase #3: %d ms", end - start));
//        return updated;
    }

}
