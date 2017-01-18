/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bulkload;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 * Usage: java bulkload.YoutubeComments
 */
public class YoutubeComments
{
    private static final String KEYSPACE = "youtube";

    private static final String TABLENAME = "youtube_comments";

    /** Default output directory */
    public static final String DEFAULT_OUTPUT_DIR = "./data";

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    // private static final int DELIMITER = ',';
    public static final int DELIMITER = Integer.parseInt("01", 16);

    private static final CsvPreference HIVE_HEX_DELIMITED = new CsvPreference.Builder('"', DELIMITER, "\n").build();

    private static final String SCHEMA = String.format(
            "CREATE TABLE %s.%s (" +
              "uid text, " +
              "video_id text, " +
              "author_id text, " +
              "reply_id text, " +
              "created_at timestamp, " +
              "statistics map<text, bigint>, " +
              "body text, " +
              "visibility text, " +
              "updates map<text, timestamp>," +
              "PRIMARY KEY (uid) " +
          ")", KEYSPACE, TABLENAME);

    private static final String INSERT_STMT = String.format(
            "INSERT INTO %s.%s (" +
            "uid, video_id, author_id, reply_id, created_at, body, visibility" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?)", KEYSPACE, TABLENAME);


    public static void main(String[] args) throws IOException, ParseException, InvalidRequestException
    {
        if (args.length < 1)
        {
            System.out.println("usage: java bulkload.YoutubeComments <csvfile>");
            return;
        }

        String csvFilename = args[0];

        System.out.println(String.format("Reading filename %s", csvFilename));

        // magic!
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + TABLENAME);
        if (!outputDir.exists() && !outputDir.mkdirs())
        {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
        }

        // Prepare SSTable writer
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();

        // File reader
        BufferedReader reader = new BufferedReader(new FileReader(csvFilename));

        // CSV Reader
        CsvListReader csvReader = new CsvListReader(reader, HIVE_HEX_DELIMITED);

        // set output directory
        builder.inDirectory(outputDir)
               .forTable(SCHEMA)
               .using(INSERT_STMT)
               .withPartitioner(new Murmur3Partitioner());

        CQLSSTableWriter writer = builder.build();

        List<String> line;
        // csvReader.getHeader(false);

        while ((line = csvReader.read()) != null) {
             // We use Java types here based on
             // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
             writer.addRow(
                line.get(0),
                line.get(1),
                line.get(2),
                line.get(3),
                DATE_FORMAT.parse(line.get(6)),
                line.get(9),
                line.get(8)
            );
        }

        try
        {
            writer.close();
        }
        catch (IOException ignore) {}
    }
}
