package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private int indexColumn;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        index = new TreeMap<>();
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN*numRows*numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int curVal = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                putIntField(rowId, colId, curVal);

                // set up index on indexColumn
                if (colId == indexColumn) {

                    IntArrayList rowIds = index.getOrDefault(curVal, null);
                    if (rowIds == null) {
                        rowIds = new IntArrayList();
                    }
                    rowIds.add(rowId);
                    index.put(curVal, rowIds);
                }
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
        long sum = 0;
        for (int i = 0;i < numRows;i++) {
            int curVal = getIntField(i, 0);
            sum += curVal;
        }
        return sum;
    }

    /**
     * Check if a value satisfies the threshold predicate.
     * @param rowId rowId
     * @param filterColumnId the column to be filtered
     * @param threshold threshold
     * @return true/false
     */
    private boolean satisfyPredicate(int rowId, int filterColumnId, int threshold) {
        int filterVal = getIntField(rowId, filterColumnId);
        if (filterColumnId == 1) {
            return filterVal > threshold;
        } else {
            return filterVal < threshold;
        }
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

        // in the case that we can utilize the index
        if (indexColumn == 1 || indexColumn == 2) {
            IntArrayList validRowIds = new IntArrayList();
            for (Integer key : index.keySet()) {
                if (indexColumn == 1 && key > threshold1) {
                    validRowIds.addAll(index.get(key));
                }
                if (indexColumn == 2) {
                    if (key < threshold2) {
                        validRowIds.addAll(index.get(key));
                    } else {
                        break;  // if key >= threshold2, no need to go on, just break
                    }
                }
            }

            for (Integer rowId : validRowIds) {
                int filterColId = indexColumn == 1 ? 2 : 1;
                int threshold = indexColumn == 1 ? threshold2 : threshold1;
                if (satisfyPredicate(rowId, filterColId, threshold)) {
                    sum += getIntField(rowId, 0);
                }
            }
            return sum;
        }

        // in the case that we can't use the index
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

        // in the case that we can utilize the index
        if (indexColumn == 0) {
            IntArrayList validRowIds = new IntArrayList();
            for (Integer key : index.keySet()) {
                if (key > threshold) {
                    validRowIds.addAll(index.get(key));
                }
            }

            for (Integer rowId : validRowIds) {
                for (int j = 0;j < numCols;j++) {
                    sum += getIntField(rowId, j);
                }
            }
            return sum;
        }

        // in the case that we can't use the index
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

        // in the case that we can utilize the index
        if (indexColumn == 0) {
            IntArrayList validRowIds = new IntArrayList();
            for (Integer key : index.keySet()) {
                if (key < threshold) {
                    validRowIds.addAll(index.get(key));
                } else {
                    break;
                }
            }

            for (Integer rowId : validRowIds) {
                int val1 = getIntField(rowId, 1);
                int val2 = getIntField(rowId, 2);
                putIntField(rowId, 3, val1 + val2);
            }
            updated = validRowIds.size();
            return updated;
        }

        // in the case that we can't use the index
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
