package pl.allegro.tech.hermes.common.metric.rate.graphite;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionMetrics;
import pl.allegro.tech.hermes.api.TopicMetrics;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.common.exception.UnavailableRateException;
import pl.allegro.tech.hermes.common.metric.rate.RateStorage;

import javax.inject.Inject;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static pl.allegro.tech.hermes.api.TopicMetrics.Builder.topicMetrics;

public class GraphiteRateStorage implements RateStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphiteRateStorage.class);

    private static final String DEFAULT_VALUE = "0.0";
    private static final String TARGET_PARAM = "target";

    private final WebTarget webTarget;
    private final GraphiteQueries queries;

    @Inject
    public GraphiteRateStorage(WebTarget webTarget, GraphiteQueries graphiteQueries) {
        this.webTarget = webTarget
                .path("render")
                .queryParam("from", "-1minutes")
                .queryParam("until", "now")
                .queryParam("format", "json");
        this.queries = graphiteQueries;
    }

    @Override
    public TopicMetrics getTopicRates(TopicName topicName) {
        try {
            return getTopicRates(queries.getTopicRateQuery(topicName), queries.getTopicRateDeliveryQuery(topicName));
        } catch (Exception exception) {
            LOGGER.warn("Error response from graphite: {}", exception.getMessage());
            throw new UnavailableRateException(topicName, exception);
        }
    }

    private TopicMetrics getTopicRates(String rateQuery, String deliveryRateQuery) {
        List<GraphiteResponse> responses = queryGraphite(rateQuery, deliveryRateQuery);
        return extractTopicMetrics(responses, rateQuery, deliveryRateQuery);
    }

    private TopicMetrics extractTopicMetrics(List<GraphiteResponse> responses, String rateQuery, String rateDeliveryQuery) {
        return topicMetrics()
                .withRate(extractValue(responses, rateQuery).or(DEFAULT_VALUE))
                .withDeliveryRate(extractValue(responses, rateDeliveryQuery).or(DEFAULT_VALUE)).build();
    }

    @Override
    public SubscriptionMetrics getSubscriptionRates(TopicName topicName, String subscriptionName) {
        try {
            String query = queries.getSubscriptionRateQuery(topicName, subscriptionName);
            return extractSubscriptionMetrics(queryGraphite(query), query);
        } catch (Exception e) {
            LOGGER.warn("Error response from graphite: {}", e.getMessage());
            throw new UnavailableRateException(topicName, subscriptionName, e);
        }
    }

    private SubscriptionMetrics extractSubscriptionMetrics(List<GraphiteResponse> responses, String rateQuery) {
        SubscriptionMetrics metrics = new SubscriptionMetrics();
        metrics.setRate(extractValue(responses, rateQuery).or(DEFAULT_VALUE));
        return metrics;
    }

    private String getFirstValue(GraphiteResponse graphiteResponse) {
        checkArgument(hasDatapoints(graphiteResponse), "Graphite format changed. Reexamine implementation.");
        String value = graphiteResponse.getDatapoints().get(0).get(0);
        return Strings.isNullOrEmpty(value) || "null".equals(value) ? DEFAULT_VALUE : value;
    }

    private boolean hasDatapoints(GraphiteResponse graphiteResponse) {
        return !graphiteResponse.getDatapoints().isEmpty() && !graphiteResponse.getDatapoints().get(0).isEmpty();
    }

    private Optional<String> extractValue(List<GraphiteResponse> responses, String target) {
        for (GraphiteResponse response : responses) {
            if (target.equals(response.getTarget())) {
                return Optional.of(getFirstValue(response));
            }
        }
        return Optional.absent();
    }

    private List<GraphiteResponse> queryGraphite(String... queries) {
        WebTarget webQuery = webTarget;
        for (String query : queries) {
            webQuery = webQuery.queryParam(TARGET_PARAM, query);
        }
        return webQuery.request(MediaType.APPLICATION_JSON).get().readEntity(new GraphiteResponseList());
    }

    protected static class GraphiteResponseList extends GenericType<List<GraphiteResponse>> {
    }
}
