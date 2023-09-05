// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.analysis;

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.load.ExportJob;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.ExportCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The `Export` sql finally generates the `Outfile` sql.
 * This test is to test whether the generated outfile sql is correct.
 */
public class ExportViewToOutfileSqlTest extends TestWithFeService {
    private String dbName = "testDb";
    private String tblName = "table1";

    /**
     * create a database and a view
     *
     * @throws Exception
     */
    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(dbName);
        useDatabase(dbName);
        createTable("create table " + tblName + "\n" + "(k1 int, k2 int, k3 int) "
                + "PARTITION BY RANGE(k1)\n" + "(\n"
                + "PARTITION p1 VALUES LESS THAN (\"20\"),\n"
                + "PARTITION p2 VALUES [(\"20\"),(\"30\")),\n"
                + "PARTITION p3 VALUES [(\"30\"),(\"40\")),\n"
                + "PARTITION p4 VALUES LESS THAN (\"50\")\n" + ")\n"
                + " distributed by hash(k1) buckets 10\n"
                + "properties(\"replication_num\" = \"1\");");

        createView("CREATE VIEW view1 (c1, c2, c3) \n"
                        + "AS\n"
                        + "SELECT k1, sum(k2), min(k3) FROM table1\n"
                        + "GROUP BY k1;");
    }

    /**
     * test normal export, sql:
     *
     * EXPORT TABLE testDb.view1
     * TO "file:///tmp/exp_"
     *
     * @throws UserException
     */
    @Test
    public void testNormal() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.view1\n"
                + "TO \"file:///tmp/exp_\";";

        // This export sql should generate 1 array, and there should be 4 outfile sql in this array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT * FROM `internal`.`default_cluster:testDb`.`view1` "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";

        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(1, outfileSqlPerParallel.size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
    }

    /**
     * test normal export, sql:
     *
     * EXPORT TABLE testDb.table1
     * TO "file:///tmp/exp_"
     * PROPERTIES(
     *     "parallelism" = "3"
     * );
     *
     * @throws UserException
     */
    @Test
    public void testNormalParallelism() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.view1\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"parallelism\" = \"4\"\n"
                + ");";

        // This export sql should generate 1 array, and there should be 4 outfile sql in this array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT * FROM `internal`.`default_cluster:testDb`.`view1` "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";

        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(1, outfileSqlPerParallel.size());
        Assert.assertEquals(1, outfileSqlPerParallel.get(0).size());
        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
    }

    /**
     * test export specified columns, sql:
     *
     * export table testDb.view1 PARTITION (p1)
     * to "file:///tmp/exp_"
     * PROPERTIES(
     *     "parallelism" = "5",
     *     "columns" = "k3, k1, k2"
     * );
     * @throws UserException
     */
    @Test
    public void testColumns() throws UserException {
        // The origin export sql
        String exportSql = "EXPORT TABLE testDb.table1 PARTITION (p1)\n"
                + "TO \"file:///tmp/exp_\" "
                + "PROPERTIES(\n"
                + "\"parallelism\" = \"5\",\n"
                + "\"columns\" = \"k3, k1, k2\""
                + ");";

        // This export sql should generate 4 array, and there should be 1 outfile sql in per array.
        // The only difference between them is the TABLET(). They are:
        String outfileSql1 = "SELECT k3, k1, k2 FROM `internal`.`default_cluster:testDb`.`table1` "
                + "TABLET(10010, 10012) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql2 = "SELECT k3, k1, k2 FROM `internal`.`default_cluster:testDb`.`table1` "
                + "TABLET(10014, 10016) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql3 = "SELECT k3, k1, k2 FROM `internal`.`default_cluster:testDb`.`table1` "
                + "TABLET(10018, 10020) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql4 = "SELECT k3, k1, k2 FROM `internal`.`default_cluster:testDb`.`table1` "
                + "TABLET(10022, 10024) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";
        String outfileSql5 = "SELECT k3, k1, k2 FROM `internal`.`default_cluster:testDb`.`table1` "
                + "TABLET(10026, 10028) "
                + "INTO OUTFILE \"file:///tmp/exp_\" FORMAT AS csv "
                + "PROPERTIES (\"column_separator\" = \"\t\", \"line_delimiter\" = \"\n"
                + "\");";

        // generate outfile
        List<List<String>> outfileSqlPerParallel = getOutfileSqlPerParallel(exportSql);

        // check
        Assert.assertEquals(5, outfileSqlPerParallel.size());
        for (int i = 0; i < 5; ++i) {
            Assert.assertEquals(1, outfileSqlPerParallel.get(i).size());
        }


        Assert.assertEquals(outfileSql1, outfileSqlPerParallel.get(0).get(0));
        Assert.assertEquals(outfileSql2, outfileSqlPerParallel.get(1).get(0));
        Assert.assertEquals(outfileSql3, outfileSqlPerParallel.get(2).get(0));
        Assert.assertEquals(outfileSql4, outfileSqlPerParallel.get(3).get(0));
        Assert.assertEquals(outfileSql5, outfileSqlPerParallel.get(4).get(0));
    }

    private LogicalPlan parseSql(String exportSql) {
        StatementBase statementBase = new NereidsParser().parseSQL(exportSql).get(0);
        return ((LogicalPlanAdapter) statementBase).getLogicalPlan();
    }

    // need open EnableNereidsPlanner
    private List<List<String>> getOutfileSqlPerParallel(String exportSql) throws UserException {
        ExportCommand exportCommand = (ExportCommand) parseSql(exportSql);
        List<List<String>> outfileSqlPerParallel = new ArrayList<>();
        try {
            Method getTableName = exportCommand.getClass().getDeclaredMethod("getTableName", ConnectContext.class);
            getTableName.setAccessible(true);

            Method checkAllParameters = exportCommand.getClass().getDeclaredMethod("checkAllParameters",
                    ConnectContext.class, TableName.class, Map.class);
            checkAllParameters.setAccessible(true);

            Method generateExportJob = exportCommand.getClass().getDeclaredMethod("generateExportJob",
                    ConnectContext.class, Map.class, TableName.class);
            generateExportJob.setAccessible(true);

            TableName tblName = (TableName) getTableName.invoke(exportCommand, connectContext);
            checkAllParameters.invoke(exportCommand, connectContext, tblName, exportCommand.getFileProperties());

            ExportJob job = (ExportJob) generateExportJob.invoke(
                    exportCommand, connectContext, exportCommand.getFileProperties(), tblName);
            outfileSqlPerParallel = job.getOutfileSqlPerParallel();
        } catch (NoSuchMethodException e) {
            throw new UserException(e);
        } catch (InvocationTargetException e) {
            throw new UserException(e);
        } catch (IllegalAccessException e) {
            throw new UserException(e);
        }
        return outfileSqlPerParallel;
    }
}
