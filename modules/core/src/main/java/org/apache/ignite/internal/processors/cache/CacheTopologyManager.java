/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap2;

/**
 *
 */
public class CacheTopologyManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /**
     * @param affAssignment Affinity assignment.
     * @param partMap Partition map.
     * @return {@code True} if partition primary nodes own partitions.
     */
    public boolean primaryOwnPartitions(List<List<ClusterNode>> affAssignment, GridDhtPartitionFullMap partMap) {
        for (int i = 0; i < affAssignment.size(); i++) {
            ClusterNode primary = affAssignment.get(i).get(0);

            GridDhtPartitionMap2 stateMap = partMap.get(primary.id());

            GridDhtPartitionState state = stateMap.get(i);

            if (state != GridDhtPartitionState.OWNING)
                return false;
        }

        return true;
    }
}
