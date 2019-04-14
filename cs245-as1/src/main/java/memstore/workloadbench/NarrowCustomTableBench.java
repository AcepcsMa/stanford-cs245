package memstore.workloadbench;

import memstore.GraderConstants;
import memstore.data.DataLoader;
import memstore.data.RandomizedLoader;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Class for custom table benchmark, with a specific initial seed
 * for random number generation.
 * Creates a RandomizedDataLoader with 5 columns and 7.5 million rows,
 * then runs predicatedUpdate(), columnSum(), predicatedColumnSum(),
 * predicatedAllColumnsSum() `numQueries` times.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class NarrowCustomTableBench extends CustomTableBenchAbstract {
    public double getExpectedTime() {
        return 2000.0;
    }

    @Override
    public DataLoader getRandomLoader() {
        int numCols = 5;
        int numRows = 15_000_000 / numCols;

        numQueries = 20;
        return new RandomizedLoader(seed, numRows, numCols);
    }

    @Setup
    public void prepare() throws IOException {
        super.prepare();
    }

    @Benchmark
    public long testQueries() {
//        return super.testQueries();

        // Make sure testQueries uses the same seed across runs.
        random = new Random(seed);

        long finalResult = 0;
        for (int i = 0; i < numQueries; i++) {
            long start = System.currentTimeMillis();
            finalResult += table.predicatedUpdate(
                    random.nextInt(upperBoundColumnValue));
            long end = System.currentTimeMillis();
            System.out.println(String.format("predicatedUpdate: %d ms", end - start));

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
