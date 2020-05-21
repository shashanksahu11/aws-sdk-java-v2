/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.enhanced.dynamodb.internal.mapper;

import java.util.Optional;

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.enhanced.dynamodb.TableMetadata;

@SdkInternalApi
public class StaticIndex implements TableMetadata.Index {
    private final String name;
    private final TableMetadata.Key partitionKey;
    private final TableMetadata.Key sortKey;

    private StaticIndex(Builder b) {
        this.name = b.name;
        this.partitionKey = b.partitionKey;
        this.sortKey = b.sortKey;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builderFrom(TableMetadata.Index index) {
        return index == null ? builder() : builder().name(index.name())
                                                    .partitionKey(index.partitionKey().orElse(null))
                                                    .sortKey(index.sortKey().orElse(null));
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public Optional<TableMetadata.Key> partitionKey() {
        return Optional.ofNullable(this.partitionKey);
    }

    @Override
    public Optional<TableMetadata.Key> sortKey() {
        return Optional.ofNullable(this.sortKey);
    }

    public static class Builder {
        private String name;
        private TableMetadata.Key partitionKey;
        private TableMetadata.Key sortKey;

        private Builder() {
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder partitionKey(TableMetadata.Key partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        public Builder sortKey(TableMetadata.Key sortKey) {
            this.sortKey = sortKey;
            return this;
        }

        public StaticIndex build() {
            return new StaticIndex(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StaticIndex that = (StaticIndex) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (partitionKey != null ? !partitionKey.equals(that.partitionKey) : that.partitionKey != null) {
            return false;
        }
        return sortKey != null ? sortKey.equals(that.sortKey) : that.sortKey == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (partitionKey != null ? partitionKey.hashCode() : 0);
        result = 31 * result + (sortKey != null ? sortKey.hashCode() : 0);
        return result;
    }
}
