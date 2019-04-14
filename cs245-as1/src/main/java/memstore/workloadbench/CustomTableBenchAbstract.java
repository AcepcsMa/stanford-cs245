package memstore.workloadbench;

import memstore.GraderConstants;
import memstore.data.DataLoader;
import memstore.data.RandomizedLoader;
import memstore.table.CustomTable;
import memstore.table.Table;

import java.io.IOException;
import java.util.Random;

/**
 * Abstract class for custom table benchmark.
 * Creates a RandomizedDataLoader with an unspecified number of rows
 * and columns, then runs predicatedUpdate(), columnSum(), predicatedColumnSum(),
 * predicatedAllColumnsSum() `numQueries` times.
 * Takes in a seed to seed random number generation.
 */
public abstract class CustomTableBenchAbstract {
    int numQueries;
    int upperBoundColumnValue = 1024;

    Table table;
    Random random;
    int seed;

    public abstract DataLoader getRandomLoader();
    public abstract double getExpectedTime();

    public void prepare() throws IOException {
        this.seed = GraderConstants.getSeed(3);
        random = new Random(seed);
        DataLoader dataLoader = getRandomLoader();

        table = new CustomTable();
        table.load(dataLoader);
    }

    public long testQueries() {
        // Make sure testQueries uses the same seed across runs.
        random = new Random(seed);

        long finalResult = 0;
        for (int i = 0; i < numQueries; i++) {
            long start = System.currentTimeMillis();
            finalResult += table.predicatedUpdate(
                    random.nextInt(upperBoundColumnValue));
//            long end = System.currentTimeMillis();
//            System.out.println(String.format("predicatedUpdate: %d ms", end - start));

            start = System.currentTimeMillis();
            finalResult += table.columnSum();
//            end = System.currentTimeMillis();
//            System.out.println(String.format("columnSum: %d ms", end - start));

            start = System.currentTimeMillis();
            finalResult += table.predicatedColumnSum(
                    random.nextInt(upperBoundColumnValue),
                    random.nextInt(upperBoundColumnValue));
//            end = System.currentTimeMillis();
//            System.out.println(String.format("predicatedColumnSum: %d ms", end - start));

            start = System.currentTimeMillis();
            finalResult += table.predicatedAllColumnsSum(
                    random.nextInt(upperBoundColumnValue));
//            end = System.currentTimeMillis();
//            System.out.println(String.format("predicatedAllColumnSum: %d ms", end - start));
        }
        return finalResult;
    }
}
