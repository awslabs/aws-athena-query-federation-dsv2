package com.amazonaws.athena.connectors.dsv2;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public abstract class AthenaFederationCatalog implements TableCatalog, AthenaFederationAdapterDefinitionProvider, SupportsNamespaces {
    private String catalogName;
    private Map<String, String> properties;
    private AthenaFederationAdapterDefinition adapterDefinition;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        com.amazonaws.athena.connector.lambda.data.DateTimeFormatterUtil.disableTimezonePacking();
        this.catalogName = name;
        this.properties = options.asCaseSensitiveMap();
        this.adapterDefinition = getAthenaFederationAdapterDefinition();
    }

    @Override
    public String name() {
        return catalogName;
    }

    // SupportsNamespaces methods
    @Override
    public String[][] listNamespaces() {
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl(ArrowUtils.rootAllocator())) {
            ListSchemasRequest request = new ListSchemasRequest(
                    adapterDefinition.getFederatedIdentity(properties),
                    adapterDefinition.getQueryId(properties),
                    adapterDefinition.getCatalogName(properties));
            ListSchemasResponse response = adapterDefinition
                    .getMetadataHandler(adapterDefinition.getFederationConfig(properties))
                    .doListSchemaNames(allocator, request);
            return response.getSchemas().stream()
                    .map(schema -> new String[]{schema})
                    .toArray(String[][]::new);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String[][] listNamespaces(String[] namespace) {
        // Athena Federation has single-level namespaces, so no nested namespaces
        return new String[0][];
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace) {
        return Collections.emptyMap();
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
        return Arrays.stream(listNamespaces())
                .anyMatch(ns -> Arrays.equals(ns, namespace));
    }

    // TableCatalog methods
    @Override
    public Identifier[] listTables(String[] namespace) {
        if (namespace.length != 1) {
            throw new IllegalArgumentException("Namespace must have exactly one level");
        }
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl(ArrowUtils.rootAllocator())) {
            ListTablesRequest request = new ListTablesRequest(
                    adapterDefinition.getFederatedIdentity(properties),
                    adapterDefinition.getQueryId(properties),
                    adapterDefinition.getCatalogName(properties),
                    namespace[0],
                    null,
                    ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE);
            ListTablesResponse response = adapterDefinition
                    .getMetadataHandler(adapterDefinition.getFederationConfig(properties))
                    .doListTables(allocator, request);
            return response.getTables().stream()
                    .map(t -> Identifier.of(new String[]{t.getSchemaName()}, t.getTableName()))
                    .toArray(Identifier[]::new);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Table loadTable(Identifier ident) {
        String schemaName = ident.namespace()[0];
        String tableName = ident.name();
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl(ArrowUtils.rootAllocator())) {
            GetTableRequest request = new GetTableRequest(
                    adapterDefinition.getFederatedIdentity(properties),
                    adapterDefinition.getQueryId(properties),
                    adapterDefinition.getCatalogName(properties),
                    new TableName(schemaName, tableName),
                    Collections.emptyMap());
            GetTableResponse response = adapterDefinition
                    .getMetadataHandler(adapterDefinition.getFederationConfig(properties))
                    .doGetTable(allocator, request);

            org.apache.arrow.vector.types.pojo.Schema updatedSchema =
                    AthenaFederationTableProvider.convertSchemaMilliFieldsToMicro(response.getSchema());
            GetTableResponse updatedResponse = new GetTableResponse(
                    response.getCatalogName(), response.getTableName(), updatedSchema, response.getPartitionColumns());

            StructType sparkSchema = ArrowUtils.fromArrowSchema(updatedSchema);
            return new AthenaFederationTable(adapterDefinition, sparkSchema, updatedResponse, new Transform[0]);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean tableExists(Identifier ident) {
        try {
            loadTable(ident);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // Unsupported write operations
    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata) {
        throw new UnsupportedOperationException("Create namespace not supported");
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) {
        throw new UnsupportedOperationException("Alter namespace not supported");
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade) {
        throw new UnsupportedOperationException("Drop namespace not supported");
    }

    @Override
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> props) {
        throw new UnsupportedOperationException("Create table not supported");
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) {
        throw new UnsupportedOperationException("Alter table not supported");
    }

    @Override
    public boolean dropTable(Identifier ident) {
        throw new UnsupportedOperationException("Drop table not supported");
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent) {
        throw new UnsupportedOperationException("Rename table not supported");
    }
}
