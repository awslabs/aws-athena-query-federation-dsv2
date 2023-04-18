/*-
 * #%L
 * athena-dynamodb-dsv2
 * %%
 * Copyright (C) 2023 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.dsv2.dynamodb;

import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connectors.dsv2.AthenaFederationAdapterDefinition;
import com.amazonaws.athena.connectors.dynamodb.DynamoDBMetadataHandler;
import com.amazonaws.athena.connectors.dynamodb.DynamoDBRecordHandler;

import java.io.Serializable;
import java.util.Map;

class DDBAdapterDefinition implements AthenaFederationAdapterDefinition, Serializable
{
    @Override
    public MetadataHandler getMetadataHandler(Map<String, String> configOptions)
    {
        return new DynamoDBMetadataHandler(configOptions);
    }

    @Override
    public RecordHandler getRecordHandler(Map<String, String> configOptions)
    {
        return new DynamoDBRecordHandler(configOptions);
    }
}
