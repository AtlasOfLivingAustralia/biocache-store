package au.org.ala.biocache.persistence;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

public class Cassandra3RetryPolicy implements RetryPolicy {

    private final int maxRetryCount;

    Cassandra3RetryPolicy(int maxRetryCount){
        this.maxRetryCount = maxRetryCount;

    }

    /**
     * {@inheritDoc}
    */
    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        if (nbRetry >=  maxRetryCount)
            return RetryDecision.rethrow();
        if (statement.isIdempotent())
            return RetryDecision.retry(cl);
        return RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        if (nbRetry >=  maxRetryCount)
            return RetryDecision.rethrow();
        if (statement.isIdempotent())
            return RetryDecision.retry(cl);
        return RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        return (nbRetry <= maxRetryCount)
                ? RetryDecision.tryNextHost(null)
                : RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
        return RetryDecision.tryNextHost(cl);
    }

    @Override
    public void init(Cluster cluster) {
        // nothing to do
    }

    @Override
    public void close() {
        // nothing to do
    }
}