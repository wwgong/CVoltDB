/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/*
 * This samples uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the database, using the standard JDBC interface
 * available for VoltDB.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 */
package voltkv;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import java.util.ArrayList;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLongArray;

// The VoltDB JDBC driver exposes extended methoed through the IVoltDBConnection interface.
// Specifically for this example, we will use the embedded performance statistics.
import org.voltdb.jdbc.IVoltDBConnection;

// This sample also use a simple utility class for command-line applications, however, note
// that there is otherwise no VoltDB-specific imports.  The sample is only using standard
// Java and JDBC statements.
import org.voltdb.client.exampleutils.AppHelper;

public class JDBCBenchmark
{
    // Initialize some common constants and variables
    private static final AtomicLongArray GetStoreResults = new AtomicLongArray(2);
    private static final AtomicLongArray GetCompressionResults = new AtomicLongArray(2);
    private static final AtomicLongArray PutStoreResults = new AtomicLongArray(2);
    private static final AtomicLongArray PutCompressionResults = new AtomicLongArray(2);

    // Reference to the database connection we will use in them main thread
    private static Connection Con;

    // Class for each thread that will be run in parallel, performing JDBC requests against the VoltDB server
    private static class ClientThread implements Runnable
    {
        private final String url;
        private final long duration;
        private final PayloadProcessor processor;
        private final double getPutRatio;
        public ClientThread(String url, PayloadProcessor processor, long duration, double getPutRatio) throws Exception
        {
            this.url = url;
            this.duration = duration;
            this.processor = processor;
            this.getPutRatio = getPutRatio;
        }

        @Override
        public void run()
        {
            // Each thread gets its dedicated JDBC connection, and posts operations against it.
            Connection con = null;
            try
            {
                con = DriverManager.getConnection(url, "", "");
                final CallableStatement getCS = con.prepareCall("{call Get(?)}");
                final CallableStatement putCS = con.prepareCall("{call Put(?,?)}");
                long endTime = System.currentTimeMillis() + (1000l * this.duration);
                Random rand = new Random();
                while (endTime > System.currentTimeMillis())
                {
                    // Decide whether to perform a GET or PUT operation
                    if (rand.nextDouble() < getPutRatio)
                    {
                        try
                        {
                            getCS.setString(1, processor.generateRandomKeyForRetrieval());
                            ResultSet result = getCS.executeQuery();
                            if (result.next())
                            {
                                final PayloadProcessor.Pair pair = processor.retrieveFromStore(result.getString(1), result.getBytes(2));
                                GetStoreResults.incrementAndGet(0);
                                GetCompressionResults.addAndGet(0, pair.getStoreValueLength());
                                GetCompressionResults.addAndGet(1, pair.getRawValueLength());
                            }
                            else
                                GetStoreResults.incrementAndGet(1);
                        }
                        catch(Exception x)
                        {
                            GetStoreResults.incrementAndGet(1);
                        }
                    }
                    else
                    {
                        final PayloadProcessor.Pair pair = processor.generateForStore();
                        try
                        {
                            // Put a key/value pair, asynchronously
                            putCS.setString(1, pair.Key);
                            putCS.setBytes(2, pair.getStoreValue());
                            putCS.executeUpdate();
                            PutStoreResults.incrementAndGet(0);
                        }
                        catch(Exception x)
                        {
                            PutStoreResults.incrementAndGet(1);
                        }
                        finally
                        {
                            PutCompressionResults.addAndGet(0, pair.getStoreValueLength());
                            PutCompressionResults.addAndGet(1, pair.getRawValueLength());
                        }
                    }
                }
            }
            catch(Exception x)
            {
                System.err.println("Exception: " + x);
                x.printStackTrace();
            }
            finally
            {
                try { con.close(); } catch (Exception x) {}
            }
        }
    }

    // Application entry point
    public static void main(String[] args)
    {
        try
        {

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Use the AppHelper utility class to retrieve command line application parameters

            // Define parameters and pull from command line
            AppHelper apph = new AppHelper(AsyncBenchmark.class.getCanonicalName())
                .add("threads", "thread_count", "Number of concurrent threads attacking the database.", 1)
                .add("display-interval", "display_interval_in_seconds", "Interval for performance feedback, in seconds.", 10)
                .add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 120)
                .add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost")
                .add("port", "port_number", "Client port to connect to on cluster nodes.", 21212)
                .add("pool-size", "pool_size", "Size of the pool of keys to work with (10,00, 10,000, 100,000 items, etc.).", 100000)
                .add("preload", "preload", "Whether the data store should be initialized with default values before the benchmark is run (true|false).", true)
                .add("get-put-ratio", "get_put_ratio", "Ratio of GET versus PUT operations: 1.0 => 100% GETs; 0.0 => 0% GETs; 0.95 => 95% GETs, 5% PUTs. Value between 0 and 1", 0.95)
                .add("key-size", "key_size", "Size of the keys in number of characters. Max: 250", 50)
                .add("min-value-size", "min_value_size", "Minimum size for the value blob (in bytes, uncompressed). Max: 1048576", 1000)
                .add("max-value-size", "max_value_size", "Maximum size for the value blob (in bytes, uncompressed) - set equal to min-value-size for constant size. Max: 1048576", 1000)
                .add("use-compression", "use_compression", "Whether value blobs should be compressed (GZip) for storage in the database (true|false).", false)
                .setArguments(args)
            ;

            // Retrieve parameters
            int threadCount        = apph.intValue("threads");
            long displayInterval   = apph.longValue("display-interval");
            long duration          = apph.longValue("duration");
            String servers         = apph.stringValue("servers");
            int port               = apph.intValue("port");
            double getPutRatio     = apph.doubleValue("get-put-ratio");
            int poolSize           = apph.intValue("pool-size");
            boolean preload        = apph.booleanValue("preload");
            int keySize            = apph.intValue("key-size");
            int minValueSize       = apph.intValue("min-value-size");
            int maxValueSize       = apph.intValue("max-value-size");
            boolean useCompression = apph.booleanValue("use-compression");
            final String csv       = apph.stringValue("stats");


            // Validate parameters
            apph.validate("duration", (duration > 0))
                .validate("display-interval", (displayInterval > 0))
                .validate("threads", (threadCount > 0))
                .validate("pool-size", (poolSize > 0))
                .validate("get-put-ratio", (getPutRatio >= 0) && (getPutRatio <= 1))
                .validate("key-size", (keySize > 0) && (keySize < 251))
                .validate("min-value-size", (minValueSize > 0) && (minValueSize < 1048576))
                .validate("max-value-size", (maxValueSize > 0) && (maxValueSize < 1048576) && (maxValueSize >= minValueSize))
            ;

            // Display actual parameters, for reference
            apph.printActualUsage();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // We need only do this once, to "hot cache" the JDBC driver reference so the JVM may realize it's there.
            Class.forName("org.voltdb.jdbc.Driver");

            // Prepare the JDBC URL for the VoltDB driver
            String url = "jdbc:voltdb://" + servers + ":" + port;

            // Get a client connection - we retry for a while in case the server hasn't started yet
            System.out.printf("Connecting to: %s\n", url);
            int sleep = 1000;
            while(true)
            {
                try
                {
                    Con = DriverManager.getConnection(url, "", "");
                    break;
                }
                catch (Exception e)
                {
                    System.err.printf("Connection failed - retrying in %d second(s).\n", sleep/1000);
                    try {Thread.sleep(sleep);} catch(Exception tie){}
                    if (sleep < 8000)
                        sleep += sleep;
                }
            }
            System.out.println("Connected.  Starting benchmark.");

            // Get a payload generator to create random Key-Value pairs to store in the database and process (uncompress) pairs retrieved from the database.
            final PayloadProcessor processor = new PayloadProcessor(keySize, minValueSize, maxValueSize, poolSize, useCompression);

            // Initialize the store
            if (preload)
            {
                System.out.print("Initializing data store... ");
                final CallableStatement initializeCS = Con.prepareCall("{call Initialize(?,?,?,?)}");
                for(int i=0;i<poolSize;i+=1000)
                {
                    initializeCS.setInt(1, i);
                    initializeCS.setInt(2, Math.min(i+1000,poolSize));
                    initializeCS.setString(3, processor.KeyFormat);
                    initializeCS.setBytes(4, processor.generateForStore().getStoreValue());
                    initializeCS.executeUpdate();
                }
                System.out.println(" Done.");
            }

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Create a Timer task to display performance data on the operating procedures
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask()
            {
                @Override
                public void run()
                {
                    try { System.out.print(Con.unwrap(IVoltDBConnection.class).getStatistics("Get", "Put")); } catch(Exception x) {}
                }
            }
            , displayInterval*1000l
            , displayInterval*1000l
            );

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Create multiple processing threads
            ArrayList<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < threadCount; i++)
                threads.add(new Thread(new ClientThread(url, processor, duration, getPutRatio)));

            // Start threads
            for (Thread thread : threads)
                thread.start();

            // Wait for threads to complete
            for (Thread thread : threads)
                thread.join();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // We're done - stop the performance statistics display task
            timer.cancel();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Now print application results:

            // 1. Store statistics as tracked by the application (ops counts, payload traffic)
            System.out.printf(
              "\n-------------------------------------------------------------------------------------\n"
            + " Store Results\n"
            + "-------------------------------------------------------------------------------------\n\n"
            + "A total of %,d operations was posted...\n"
            + " - GETs: %,9d Operations (%,9d Misses/Failures)\n"
            + "         %,9d MB in compressed store data\n"
            + "         %,9d MB in uncompressed application data\n"
            + "         Network Throughput: %6.3f Gbps*\n\n"
            + " - PUTs: %,9d Operations (%,9d Failures)\n"
            + "         %,9d MB in compressed store data\n"
            + "         %,9d MB in uncompressed application data\n"
            + "         Network Throughput: %6.3f Gbps*\n\n"
            + " - Total Network Throughput: %6.3f Gbps*\n\n"
            + "* Figure includes key & value traffic but not database protocol overhead.\n"
            + "\n"
            + "-------------------------------------------------------------------------------------\n"
            , GetStoreResults.get(0)+GetStoreResults.get(1)+PutStoreResults.get(0)+PutStoreResults.get(1)
            , GetStoreResults.get(0)
            , GetStoreResults.get(1)
            , GetCompressionResults.get(0)/1048576l
            , GetCompressionResults.get(1)/1048576l
            , ((double)GetCompressionResults.get(0) + (GetStoreResults.get(0)+GetStoreResults.get(1))*keySize)/(134217728d*(double)duration)
            , PutStoreResults.get(0)
            , PutStoreResults.get(1)
            , PutCompressionResults.get(0)/1048576l
            , PutCompressionResults.get(1)/1048576l
            , ((double)PutCompressionResults.get(0) + (PutStoreResults.get(0)+PutStoreResults.get(1))*keySize)/(134217728d*(double)duration)
            , ((double)GetCompressionResults.get(0) + (GetStoreResults.get(0)+GetStoreResults.get(1))*keySize)/(134217728d*(double)duration)
            + ((double)PutCompressionResults.get(0) + (PutStoreResults.get(0)+PutStoreResults.get(1))*keySize)/(134217728d*(double)duration)
            );

            // 2. Overall performance statistics for GET/PUT operations
            System.out.println(
              "\n\n-------------------------------------------------------------------------------------\n"
            + " System Statistics\n"
            + "-------------------------------------------------------------------------------------\n\n");
            System.out.print(Con.unwrap(IVoltDBConnection.class).getStatistics("Get", "Put").toString(false));

            // 3. Per-procedure detailed performance statistics
            System.out.println(
              "\n\n-------------------------------------------------------------------------------------\n"
            + " Detailed Statistics\n"
            + "-------------------------------------------------------------------------------------\n\n");
            System.out.print(Con.unwrap(IVoltDBConnection.class).getStatistics().toString(false));

            // Dump statistics to a CSV file
            Con.unwrap(IVoltDBConnection.class).saveStatistics(csv);

            Con.close();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

        }
        catch(Exception x)
        {
            System.out.println("Exception: " + x);
            x.printStackTrace();
        }
    }
}
