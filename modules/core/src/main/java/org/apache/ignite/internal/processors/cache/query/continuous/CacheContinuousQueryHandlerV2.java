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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Continuous query handler V2 version. Contains {@link Factory} for remote listener.
 */
public class CacheContinuousQueryHandlerV2<K, V> extends CacheContinuousQueryHandler<K, V> {
    /** Remote filter factory. */
    private Factory<? extends CacheEntryEventSerializableFilter<K, V>> rmtFilterFactory;

    /** Deployable object for filter factory. */
    private DeployableObject rmtFilterFactoryDep;

    /**
     * Required by {@link Externalizable}.
     */
    public CacheContinuousQueryHandlerV2() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheName Cache name.
     * @param topic Topic for ordered messages.
     * @param locLsnr Local listener.
     * @param rmtFilterFactory Remote filter factory.
     * @param internal Internal flag.
     * @param notifyExisting Notify existing flag.
     * @param oldValRequired Old value required flag.
     * @param sync Synchronous flag.
     * @param ignoreExpired Ignore expired events flag.
     * @param skipPrimaryCheck Whether to skip primary check for REPLICATED cache.
     * @param taskHash Task name hash code.
     * @param locCache {@code True} if local cache.
     * @param keepBinary Keep binary flag.
     */
    public CacheContinuousQueryHandlerV2(
        String cacheName,
        Object topic,
        CacheEntryUpdatedListener<K, V> locLsnr,
        CacheEntryEventSerializableFilter<K, V> rmtFilter,
        Factory<? extends CacheEntryEventSerializableFilter<K, V>> rmtFilterFactory,
        boolean internal,
        boolean notifyExisting,
        boolean oldValRequired,
        boolean sync,
        boolean ignoreExpired,
        int taskHash,
        boolean skipPrimaryCheck,
        boolean locCache,
        boolean keepBinary,
        boolean ignoreClassNotFound) {
        super(cacheName, topic, locLsnr, rmtFilter, internal, notifyExisting, oldValRequired, sync, ignoreExpired,
            taskHash, skipPrimaryCheck, locCache, keepBinary, ignoreClassNotFound);

        assert rmtFilter != null ^ rmtFilterFactory != null || rmtFilter == null && rmtFilterFactory == null;

        this.rmtFilterFactory = rmtFilterFactory;

        if (rmtFilterFactory != null)
            this.rmtFilter = rmtFilterFactory.create();
    }

    /** {@inheritDoc} */
    @Override public void p2pMarshal(GridKernalContext ctx) throws IgniteCheckedException {
        super.p2pMarshal(ctx);

        if (rmtFilterFactory != null && !U.isGrid(rmtFilterFactory.getClass()))
            rmtFilterFactoryDep = new DeployableObject(rmtFilterFactory, ctx);
    }

    /** {@inheritDoc} */
    @Override public void p2pUnmarshal(UUID nodeId, GridKernalContext ctx) throws IgniteCheckedException {
        super.p2pUnmarshal(nodeId, ctx);

        if (rmtFilterFactoryDep != null) {
            rmtFilterFactory = rmtFilterFactoryDep.unmarshal(nodeId, ctx);

            rmtFilter = rmtFilterFactory.create();
        }
    }

    /** {@inheritDoc} */
    @Override public GridContinuousHandler clone() {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryHandlerV2.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, cacheName);
        out.writeObject(topic);

        if (rmtFilterFactory == null && rmtFilter != null) {
            boolean b = rmtFilterDep != null;

            out.writeBoolean(b);

            if (b)
                out.writeObject(rmtFilterDep);
            else
                out.writeObject(rmtFilter);
        }
        else {
            out.writeBoolean(false);

            out.writeObject(null);
        }

        out.writeBoolean(internal);
        out.writeBoolean(notifyExisting);
        out.writeBoolean(oldValRequired);
        out.writeBoolean(sync);
        out.writeBoolean(ignoreExpired);
        out.writeInt(taskHash);

        boolean b = rmtFilterFactoryDep != null;

        out.writeBoolean(b);

        if (b)
            out.writeObject(rmtFilterFactoryDep);
        else
            out.writeObject(rmtFilterFactory);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = U.readString(in);
        topic = in.readObject();

        boolean b = in.readBoolean();

        if (b)
            rmtFilterDep = (DeployableObject)in.readObject();
        else
            rmtFilter = (CacheEntryEventSerializableFilter<K, V>)in.readObject();

        internal = in.readBoolean();
        notifyExisting = in.readBoolean();
        oldValRequired = in.readBoolean();
        sync = in.readBoolean();
        ignoreExpired = in.readBoolean();
        taskHash = in.readInt();

        cacheId = CU.cacheId(cacheName);

        b = in.readBoolean();

        if (b)
            rmtFilterFactoryDep = (DeployableObject)in.readObject();
        else
            rmtFilterFactory = (Factory<CacheEntryEventSerializableFilter<K, V>>)in.readObject();

        if (rmtFilter == null && rmtFilterFactory != null)
            rmtFilter = rmtFilterFactory.create();
    }
}
