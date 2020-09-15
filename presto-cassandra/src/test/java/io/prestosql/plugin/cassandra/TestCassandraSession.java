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
package io.prestosql.plugin.cassandra;

import com.datastax.driver.core.ProtocolVersion;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.prestosql.plugin.cassandra.CassandraTestingUtils.createKeyspace;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestCassandraSession
{
    private static final Logger LOG = Logger.get(TestCassandraSession.class);
    private static final String KEYSPACE = "test_native_cassandra_session_keyspace";
    private static final int FILTER_PARTITION_COUNT = 5;
    private static final int EXISTING_PARTITION_COUNT = 4;
    private static final int CLUSTERING_KEY_COUNT = 3;

    private CassandraSession session;
    private CassandraServer server;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        server = new CassandraServer("cassandra:3.11.3", ProtocolVersion.V4);
        session = server.getSession();
        createKeyspace(session, KEYSPACE);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        server.close();
    }

    @Test
    public void testGetPartitionsFromSinglePartitionKeyTable()
    {
        CassandraSession session = buildSession(false);
        String tableName = "single_part_key_table";
        CassandraTable table = createSinglePartitionKeyTable(tableName);

        ImmutableList<Set<Object>> partitionKeysList = buildSinglePartitionKeysList();
        List<CassandraPartition> partitions = session.getPartitions(table, partitionKeysList);

        assertEquals(partitions.size(), EXISTING_PARTITION_COUNT);
        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }

    @Test
    public void testGetPartitionsFromSinglePartitionKeyTableWithSkipPartitionCheck()
    {
        CassandraSession session = buildSession(true);
        String tableName = "single_part_key_with_skip_partition_check_table";
        CassandraTable table = createSinglePartitionKeyTable(tableName);

        ImmutableList<Set<Object>> partitionKeysList = buildSinglePartitionKeysList();
        List<CassandraPartition> partitions = session.getPartitions(table, partitionKeysList);

        assertEquals(partitions.size(), FILTER_PARTITION_COUNT);
        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }

    @Test
    public void testGetPartitionsFromMultiplePartitionKeyTable()
    {
        CassandraSession session = buildSession(false);
        String tableName = "multi_part_key_table";
        CassandraTable table = createMultiplePartitionKeyTable(tableName);

        ImmutableList<Set<Object>> partitionKeysList = buildMultiplePartitionKeysList();
        List<CassandraPartition> partitions = session.getPartitions(table, partitionKeysList);

        assertEquals(partitions.size(), EXISTING_PARTITION_COUNT);
        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }

    @Test
    public void testGetPartitionsFromMultiplePartitionKeyTableWithSkipPartitionCheck()
    {
        CassandraSession session1 = buildSession(false);
        CassandraSession session2 = buildSession(true);
        String tableName = "multi_part_key_with_skip_partition_check_table";
        CassandraTable table = createMultiplePartitionKeyTable(tableName);

        ImmutableList<Set<Object>> partitionKeysList = buildMultiplePartitionKeysList();
        Set<CassandraPartition> partitions1 = new HashSet<>(session1.getPartitions(table, partitionKeysList));
        Set<CassandraPartition> partitions2 = new HashSet<>(session2.getPartitions(table, partitionKeysList));

        assertEquals(partitions1.size(), EXISTING_PARTITION_COUNT);
        assertEquals(partitions2.size(), (int) Math.pow(FILTER_PARTITION_COUNT, 3));

        partitions2.retainAll(partitions1);
        assertEquals(partitions1, partitions2);
        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }

    private CassandraSession buildSession(boolean skipPartitionCheck)
    {
        return new CassandraSession(
                JsonCodec.listJsonCodec(ExtraColumnMetadata.class),
                server.getCluster(),
                new Duration(1, MINUTES),
                skipPartitionCheck);
    }

    private ImmutableList<Set<Object>> buildSinglePartitionKeysList()
    {
        ImmutableSet.Builder<Object> partitionColumnValues = ImmutableSet.builder();
        for (int i = 0; i < FILTER_PARTITION_COUNT; i++) {
            partitionColumnValues.add((long) i);
        }
        return ImmutableList.of(partitionColumnValues.build());
    }

    private CassandraTable createSinglePartitionKeyTable(String tableName)
    {
        session.execute(format("CREATE TABLE %s.%s (partition_key1 bigint, clustering_key1 bigint, PRIMARY KEY (partition_key1, clustering_key1))", KEYSPACE, tableName));
        for (int i = 0; i < EXISTING_PARTITION_COUNT; i++) {
            for (int j = 0; j < CLUSTERING_KEY_COUNT; j++) {
                session.execute(format("INSERT INTO %s.%s (partition_key1, clustering_key1) VALUES (%d, %d)", KEYSPACE, tableName, i, j));
            }
        }

        CassandraColumnHandle col1 = new CassandraColumnHandle("partition_key1", 1, CassandraType.BIGINT, true, false, false, false);
        CassandraColumnHandle col2 = new CassandraColumnHandle("clustering_key1", 2, CassandraType.BIGINT, false, true, false, false);
        return new CassandraTable(new CassandraTableHandle(KEYSPACE, tableName), ImmutableList.of(col1, col2));
    }

    private ImmutableList<Set<Object>> buildMultiplePartitionKeysList()
    {
        ImmutableSet.Builder<Object> col1Values = ImmutableSet.builder();
        ImmutableSet.Builder<Object> col2Values = ImmutableSet.builder();
        ImmutableSet.Builder<Object> col3Values = ImmutableSet.builder();
        for (int i = 0; i < FILTER_PARTITION_COUNT; i++) {
            col1Values.add((long) i);
            col2Values.add(Slices.utf8Slice(Integer.toString(i)));
            col3Values.add((long) i);
        }
        return ImmutableList.of(col1Values.build(), col2Values.build(), col3Values.build());
    }

    private CassandraTable createMultiplePartitionKeyTable(String tableName)
    {
        session.execute(format("CREATE TABLE %s.%s (partition_key1 bigint, partition_key2 text, partition_key3 date, clustering_key1 bigint, PRIMARY KEY ((partition_key1, partition_key2, partition_key3), clustering_key1))", KEYSPACE, tableName));
        for (int i = 0; i < EXISTING_PARTITION_COUNT; i++) {
            for (int j = 0; j < CLUSTERING_KEY_COUNT; j++) {
                String date = DateTimeFormatter.ISO_DATE.format(LocalDateTime.ofEpochSecond(TimeUnit.SECONDS.convert(i, TimeUnit.DAYS), 0, ZoneOffset.UTC));
                String cql = format("INSERT INTO %s.%s (partition_key1, partition_key2, partition_key3, clustering_key1) VALUES (%d, '%s', '%s', %d)", KEYSPACE, tableName, i, i, date, j);
                LOG.info(cql);
                session.execute(cql);
            }
        }

        CassandraColumnHandle col1 = new CassandraColumnHandle("partition_key1", 1, CassandraType.BIGINT, true, false, false, false);
        CassandraColumnHandle col2 = new CassandraColumnHandle("partition_key2", 2, CassandraType.TEXT, true, false, false, false);
        CassandraColumnHandle col3 = new CassandraColumnHandle("partition_key3", 3, CassandraType.DATE, true, false, false, false);
        CassandraColumnHandle col4 = new CassandraColumnHandle("clustering_key1", 4, CassandraType.BIGINT, false, true, false, false);
        return new CassandraTable(new CassandraTableHandle(KEYSPACE, tableName), ImmutableList.of(col1, col2, col3, col4));
    }
}
