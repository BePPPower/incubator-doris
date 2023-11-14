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

package org.apache.doris.planner.external.trino.connectors;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.catalog.external.TrinoColumnMetadata;
import org.apache.doris.catalog.external.TrinoConnectorExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.trino.connector.TrinoConnectorExternalCatalog;
import org.apache.doris.trino.connector.LocalQueryRunner;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.FileQueryScanNode;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TTrinoConnectorFileDesc;

import avro.shaded.com.google.common.base.Preconditions;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.Session;
import io.trino.execution.Lifespan;
import io.trino.execution.QueryIdGenerator;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import static io.trino.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import io.trino.spi.security.Identity;
import io.trino.split.SplitSource;
import io.trino.sql.SqlPath;
import io.trino.testing.TestingSession;
import static java.util.Locale.ENGLISH;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TrinoConnectorScanNode extends FileQueryScanNode {

    private static TrinoConnectorSource source = null;
    // private static List<Predicate> predicates;

    private LocalQueryRunner localQueryRunner;

    // private Session session;
    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

    public TrinoConnectorScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "TRINO_CONNECTOR_SCAN_NODE", StatisticalType.TRINO_CONNECTOR_SCAN_NODE, needCheckColumnPriv);
    }

    @Override
    protected void doInitialize() throws UserException {
        ExternalTable table = (ExternalTable) desc.getTable();
        if (table.isView()) {
            throw new AnalysisException(
                    String.format("Querying external view '%s.%s' is not supported", table.getDbName(),
                            table.getName()));
        }
        computeColumnsFilter();
        initBackendPolicy();
        localQueryRunner = Env.getCurrentEnv().getLocalQueryRunner();
        // session = ConnectContext.get().getSessionContext();
        source = new TrinoConnectorSource((TrinoConnectorExternalTable) table, desc, columnNameToRange, localQueryRunner);
        Preconditions.checkNotNull(source);
        initSchemaParams();
        // TrinoConnectorPredicateConverter trinoConnectorPredicateConverter = new TrinoConnectorPredicateConverter(
        //         source.getTrinoConnectorTable().rowType());
        // predicates = trinoConnectorPredicateConverter.convertToPaimonExpr(conjuncts);


    }

    private static final Base64.Encoder BASE64_ENCODER =
            Base64.getUrlEncoder().withoutPadding();

    private static <T> String encodeObjectToString(T t, ObjectMapperProvider objectMapperProvider) {
        try {
            io.airlift.json.JsonCodec<T> jsonCodec = (io.airlift.json.JsonCodec<T>) new JsonCodecFactory(objectMapperProvider).jsonCodec(t.getClass());
            return jsonCodec.toJson(t);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void setTrinoConnectorParams(TFileRangeDesc rangeDesc, TrinoConnectorSplit trinoConnectorSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(trinoConnectorSplit.getTableFormatType().value());
        TTrinoConnectorFileDesc fileDesc = new TTrinoConnectorFileDesc();
        fileDesc.setTrinoConnectorSplit(encodeObjectToString(trinoConnectorSplit.getSplit(), source.getLocalQueryRunner().getObjectMapperProvider()));
        // fileDesc.setTrinoConnectorPredicate(encodeObjectToString(predicates));
        fileDesc.setTrinoConnectorColumnNames(source.getDesc().getSlots().stream().map(slot -> slot.getColumn().getName())
                .collect(Collectors.joining(",")));
        fileDesc.setCatalogName(source.getCatalog().getName());
        fileDesc.setDbName(((TrinoConnectorExternalTable) source.getTargetTable()).getDbName());
        fileDesc.setTrinoConnectorOptions(((TrinoConnectorExternalCatalog) source.getCatalog()).getTrinoConnectorOptionsMap());
        fileDesc.setTableName(source.getTargetTable().getName());
        fileDesc.setTrinoConnectorTableHandle(encodeObjectToString(((TrinoConnectorExternalTable) source.getTargetTable()).getOriginTable(), source.getLocalQueryRunner()
                .getObjectMapperProvider()));
        List<String> cols = source.getDesc().getSlots().stream().map(slot -> slot.getColumn().getName())
                .collect(Collectors.toList());
        Map<String, ColumnMetadata> columnMetadataMap = ((TrinoConnectorExternalTable) source.getTargetTable()).getColumnMetadataMap();
        Map<String, ColumnHandle> columnHandleMap = ((TrinoConnectorExternalTable) source.getTargetTable()).getColumnHandleMap();
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        List<ColumnHandle> columnHandles = new ArrayList<>();
        for (SlotDescriptor slotDescriptor : source.getDesc().getSlots()) {
            String colName = slotDescriptor.getColumn().getName();
            if (columnMetadataMap.containsKey(colName)) {
                columnMetadataList.add(columnMetadataMap.get(colName));
                columnHandles.add(columnHandleMap.get(colName));
            }
        }
        fileDesc.setTrinoConnectorColumnHandles(encodeObjectToString(columnHandles, source.getLocalQueryRunner()
                .getObjectMapperProvider()));
        fileDesc.setTrinoConnectorColumnMetadata(encodeObjectToString(columnMetadataList.stream().map(
                x->new TrinoColumnMetadata(x.getName(), x.getType(), x.isNullable(), x.getComment(), x.getExtraInfo(), x.isHidden(), x.getProperties())).collect(
                Collectors.toList()), source.getLocalQueryRunner()
                .getObjectMapperProvider()));
        tableFormatFileDesc.setTrinoConnectorParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    private static List<io.trino.metadata.Split> getNextBatch(SplitSource splitSource)
    {
        return getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), 1000)).getSplits();
    }

    @Override
    public List<Split> getSplits() throws UserException {
        // Session session = TestingSession.testSessionBuilder()
        //         .build();
        Session session = Session.builder(localQueryRunner.getDefaultSession()).setQueryId(queryIdGenerator.createNextQueryId()).build();
        return localQueryRunner.getSplits(
                session,
                new QualifiedObjectName(source.getTrinoConnectorTable().getCatalogName().getCatalogName() , source.getTargetTable().getDatabase().getFullName(), source.getTargetTable().getName()),
                UNGROUPED_SCHEDULING,
                EMPTY,
                alwaysTrue()).stream().map(split->new TrinoConnectorSplit(split)).collect(Collectors.toList());
    }

    //When calling 'setTrinoConnectorParams' and 'getSplits', the column trimming has not been performed yet,
    // Therefore, trino_connector_column_names is temporarily reset here
    @Override
    public void updateRequiredSlots(PlanTranslatorContext planTranslatorContext,
            Set<SlotId> requiredByProjectSlotIdSet) throws UserException {
        super.updateRequiredSlots(planTranslatorContext, requiredByProjectSlotIdSet);
        String cols = desc.getSlots().stream().map(slot -> slot.getColumn().getName())
                .collect(Collectors.joining(","));
        for (TScanRangeLocations tScanRangeLocations : scanRangeLocations) {
            List<TFileRangeDesc> ranges = tScanRangeLocations.scan_range.ext_scan_range.file_scan_range.ranges;
            for (TFileRangeDesc tFileRangeDesc : ranges) {
                tFileRangeDesc.table_format_params.trino_connector_params.setTrinoConnectorColumnNames(cols);
            }
        }
    }

    @Override
    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        return getLocationType("");
    }

    @Override
    public TFileType getLocationType(String location) throws DdlException, MetaNotFoundException {
        //todo: no use
        return TFileType.FILE_S3;
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return new ArrayList<>();
    }

    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        return source.getFileAttributes();
    }

    @Override
    public TableIf getTargetTable() {
        return source.getTargetTable();
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        return source.getCatalog().getCatalogProperty().getHadoopProperties();
    }

}
