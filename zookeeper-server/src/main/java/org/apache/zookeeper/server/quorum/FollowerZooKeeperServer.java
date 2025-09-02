/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.management.JMException;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.metrics.MetricsContext;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Just like the standard ZooKeeperServer. We just replace the request
 * processors: FollowerRequestProcessor -&gt; CommitProcessor -&gt;
 * FinalRequestProcessor
 *
 * A SyncRequestProcessor is also spawned off to log proposals from the leader.
 */
public class FollowerZooKeeperServer extends LearnerZooKeeperServer {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerZooKeeperServer.class);

    /*
     * Pending sync requests
     */ ConcurrentLinkedQueue<Request> pendingSyncs;

    /**
     * @throws IOException
     */
    FollowerZooKeeperServer(FileTxnSnapLog logFactory, QuorumPeer self, ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout, self.maxSessionTimeout, self.clientPortListenBacklog, zkDb, self);
        this.pendingSyncs = new ConcurrentLinkedQueue<>();
    }

    public Follower getFollower() {
        return self.follower;
    }

    /**
     分析要点（简洁）

     `setupRequestProcessors` 在 `FollowerZooKeeperServer` 中创建并启动了四个关键处理器（两条逻辑链）：
     - `FinalRequestProcessor finalProcessor`：最终应用事务（更新内存/session、响应客户端）。
     - `CommitProcessor commitProcessor`：负责 commit 阶段的排队/分发，后续交给 finalProcessor。
     - `FollowerRequestProcessor firstProcessor`：作为“第一道”处理器，接收来自本地/learner 注入的请求并转发到 commitProcessor。
     - `SyncRequestProcessor syncProcessor`（配套 `SendAckRequestProcessor`）：负责记录/落盘 proposal，并向 leader 发送 ACK。

     两条主要调用链（对应 Leader->Follower 的 PROPOSAL / COMMIT 流程）：

     1) 收到 PROPOSAL 的处理链（记录并 ACK）
     - 网络层：`RecvWorker` 从 socket 读到 PROPOSAL 数据，最终由 Learner/LearnerHandler 将其封装成 `Request`。
     - 封装后调用：`FollowerZooKeeperServer.logRequest(request)`（见类中实现）
     - `pendingTxns.add(request)` （把请求放入 pendingTxns 队列，用以 later 与 commit 匹配）
     - `syncProcessor.processRequest(request)` —— 将请求交给 `SyncRequestProcessor`
     - `SyncRequestProcessor`（职责）
     - 将 proposal 写入事务日志（落盘/同步），确保本地持久化
     - 处理完后调用其下一个处理器：`SendAckRequestProcessor`（由构造时传入）
     - `SendAckRequestProcessor`（职责）
     - 向 leader 发送 ACK（表明 follower 已经记录 proposal）
     - （通常不会把请求推进 commitProcessor；commit 由 leader 下发后走另一路）

     2) 收到 COMMIT 的处理链（真正提交并应用）
     - 网络层：当 follower 收到 leader 的 COMMIT（通过 LearnerHandler），会调用 `FollowerZooKeeperServer.commit(zxid)`
     - `commit(zxid)` 会检查 `pendingTxns` 队列头 zxid 是否匹配，取出对应 `Request`，并调用：
     - `request.logLatency(...)`（统计）
     - `commitProcessor.commit(request)`
     - `CommitProcessor`（职责）
     - 接收 commit 请求后入队/排期，按顺序调用下一个处理器（`FinalRequestProcessor`）去执行 commit（实际 apply）
     - `FinalRequestProcessor`（职责）
     - 应用事务到 `ZKDatabase`（更新数据树、session、触发 watches 等）
     - 最终对客户端进行响应、完成请求生命周期

     另外的调用点（`FollowerRequestProcessor`）
     - `firstProcessor = new FollowerRequestProcessor(this, commitProcessor)`：
     - 用于接收并处理那些需要注入到提交链中的“本地/learner”请求（例如 observer 注入的请求）
     - `processObserverRequest(Request)` 会调用 `((FollowerRequestProcessor) firstProcessor).processRequest(request, false)`，随后该 processor 会把请求按需要转给 `commitProcessor`（进入 commit->final 链）

     涉及的队列 / 同步点
     - `pendingTxns`：存放已记录但尚未 commit 的 Request（由 logRequest 添加，commit 方法按顺序匹配并移除）
     - `pendingSyncs`：用于 sync（LearnerSync 的场景）
     - `syncProcessor` 与 `commitProcessor` 是并行但协调的两个职责：sync 用于落盘并 ACK，commit 用于真正应用（由 leader 的 COMMIT 驱动）

     简短结论
     - PROPOSAL 流：LearnerHandler -> logRequest() -> pendingTxns + SyncRequestProcessor -> SendAckRequestProcessor（发送 ACK）
     - COMMIT 流：LearnerHandler（接到 COMMIT）-> FollowerZooKeeperServer.commit(zxid) -> CommitProcessor.commit(request) -> FinalRequestProcessor（应用事务）
     - `FollowerRequestProcessor` 是用于把某些本地/外部注入请求引入 commit->final 链的“入口”处理器。
     */
    @Override
    protected void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        commitProcessor = new CommitProcessor(finalProcessor, Long.toString(getServerId()), true, getZooKeeperServerListener());
        commitProcessor.start();
        firstProcessor = new FollowerRequestProcessor(this, commitProcessor);
        ((FollowerRequestProcessor) firstProcessor).start();
        syncProcessor = new SyncRequestProcessor(this, new SendAckRequestProcessor(getFollower()));
        syncProcessor.start();
    }

    LinkedBlockingQueue<Request> pendingTxns = new LinkedBlockingQueue<>();

    public void logRequest(Request request) {
        if ((request.zxid & 0xffffffffL) != 0) {
            pendingTxns.add(request);
        }
        syncProcessor.processRequest(request);
    }

    /**
     * Append txn request to the transaction log directly without go through request processors.
     */
    public void appendRequest(Request request) throws IOException {
        getZKDatabase().append(request);
    }

    /**
     * When a COMMIT message is received, eventually this method is called,
     * which matches up the zxid from the COMMIT with (hopefully) the head of
     * the pendingTxns queue and hands it to the commitProcessor to commit.
     * @param zxid - must correspond to the head of pendingTxns if it exists
     */
    public void commit(long zxid) {
        if (pendingTxns.size() == 0) {
            LOG.warn("Committing " + Long.toHexString(zxid) + " without seeing txn");
            return;
        }
        long firstElementZxid = pendingTxns.element().zxid;
        if (firstElementZxid != zxid) {
            LOG.error("Committing zxid 0x" + Long.toHexString(zxid)
                      + " but next pending txn 0x" + Long.toHexString(firstElementZxid));
            ServiceUtils.requestSystemExit(ExitCode.UNMATCHED_TXN_COMMIT.getValue());
        }
        Request request = pendingTxns.remove();
        request.logLatency(ServerMetrics.getMetrics().COMMIT_PROPAGATION_LATENCY);
        commitProcessor.commit(request);
    }

    public synchronized void sync() {
        if (pendingSyncs.size() == 0) {
            LOG.warn("Not expecting a sync.");
            return;
        }

        Request r = pendingSyncs.remove();
        if (r instanceof LearnerSyncRequest) {
            LearnerSyncRequest lsr = (LearnerSyncRequest) r;
            lsr.fh.queuePacket(new QuorumPacket(Leader.SYNC, 0, null, null));
        }
        commitProcessor.commit(r);
    }

    @Override
    public int getGlobalOutstandingLimit() {
        int divisor = self.getQuorumSize() > 2 ? self.getQuorumSize() - 1 : 1;
        int globalOutstandingLimit = super.getGlobalOutstandingLimit() / divisor;
        return globalOutstandingLimit;
    }

    @Override
    public String getState() {
        return "follower";
    }

    @Override
    public Learner getLearner() {
        return getFollower();
    }

    /**
     * Process a request received from external Learner through the LearnerMaster
     * These requests have already passed through validation and checks for
     * session upgrade and can be injected into the middle of the pipeline.
     *
     * @param request received from external Learner
     */
    void processObserverRequest(Request request) {
        ((FollowerRequestProcessor) firstProcessor).processRequest(request, false);
    }

    boolean registerJMX(LearnerHandlerBean handlerBean) {
        try {
            MBeanRegistry.getInstance().register(handlerBean, jmxServerBean);
            return true;
        } catch (JMException e) {
            LOG.warn("Could not register connection", e);
        }
        return false;
    }

    @Override
    protected void registerMetrics() {
        super.registerMetrics();

        MetricsContext rootContext = ServerMetrics.getMetrics().getMetricsProvider().getRootContext();

        rootContext.registerGauge("synced_observers", self::getSynced_observers_metric);

    }

    @Override
    protected void unregisterMetrics() {
        super.unregisterMetrics();

        MetricsContext rootContext = ServerMetrics.getMetrics().getMetricsProvider().getRootContext();
        rootContext.unregisterGauge("synced_observers");

    }
}
