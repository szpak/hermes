package pl.allegro.tech.hermes.common.metric.rate.graphite;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import pl.allegro.tech.hermes.api.SubscriptionMetrics;
import pl.allegro.tech.hermes.api.TopicMetrics;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.common.exception.UnavailableRateException;
import pl.allegro.tech.hermes.common.metric.PathsCompiler;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.Date;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

public class GraphiteRateStorageTest {

    private static final TopicName TOPIC_NAME = new TopicName("group", "topicName");
    private static final String RATE = "18000.00";
    private static final String SUBSCRIPTION_NAME = "subscriptionName";
    private static final String DELIVERY_RATE = "25000.0";

    private static final int GRAPHITE_HTTP_PORT = 8079;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(GRAPHITE_HTTP_PORT);

    private WebTarget webTarget = ClientBuilder.newClient().target(String.format("http://localhost:%s", GRAPHITE_HTTP_PORT));

    private PathsCompiler pathsCompiler;
    private GraphiteQueries queries;
    private GraphiteRateStorage rateStorage;

    private String topicRateQuery;
    private String topicDeliveryRateQuery;
    private String subscriptionRateQuery;

    @Before
    public void setUp() {
        webTarget = ClientBuilder
                .newClient()
                .register(JacksonJsonProvider.class)
                .target(String.format("http://localhost:%s", GRAPHITE_HTTP_PORT));

        pathsCompiler = new PathsCompiler("localhost");
        queries = new GraphiteQueries("stats.tech.hermes", pathsCompiler);
        rateStorage = new GraphiteRateStorage(webTarget, queries);

        topicRateQuery = queries.getTopicRateQuery(TOPIC_NAME);
        topicDeliveryRateQuery = queries.getTopicRateDeliveryQuery(TOPIC_NAME);
        subscriptionRateQuery = queries.getSubscriptionRateQuery(TOPIC_NAME, SUBSCRIPTION_NAME);
    }

    @Test
    public void shouldGetTopicRates() {
        mockGraphite(
                String.format("target=%s&target=%s", topicRateQuery, topicDeliveryRateQuery),
                arrayJsonResponse(topicRateQuery, topicDeliveryRateQuery, RATE, DELIVERY_RATE)
        );

        TopicMetrics metrics = rateStorage.getTopicRates(TOPIC_NAME);

        assertThat(metrics.getRate()).isEqualTo(RATE);
        assertThat(metrics.getDeliveryRate()).isEqualTo(DELIVERY_RATE);
    }

    @Test
    public void shouldGetSubscriptionRates() {
        mockGraphite(
                String.format("target=%s", subscriptionRateQuery),
                arrayJsonResponse(subscriptionRateQuery, DELIVERY_RATE)
        );

        SubscriptionMetrics metrics = rateStorage.getSubscriptionRates(TOPIC_NAME, SUBSCRIPTION_NAME);

        assertThat(metrics.getRate()).isEqualTo(DELIVERY_RATE);
    }

    @Test(expected = UnavailableRateException.class)
    public void shouldGetUnavailableRateExceptionForTopicRates() {
        rateStorage.getTopicRates(TOPIC_NAME);
    }

    @Test(expected = UnavailableRateException.class)
    public void shouldGetUnavailableRateExceptionForSubscriptionRates() {
        rateStorage.getSubscriptionRates(TOPIC_NAME, SUBSCRIPTION_NAME);
    }

    private void mockGraphite(String targetParams, String jsonResponse) {
        stubFor(get(urlEqualTo(String.format("/render?from=-1minutes&until=now&format=json&%s", targetParams)))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON)
                        .withBody(jsonResponse)));
    }

    private String arrayJsonResponse(String query1, String query2, String rate1, String rate2) {
        return String.format("[%s,%s]", jsonResponse(query1, rate1), jsonResponse(query2, rate2));
    }

    private String arrayJsonResponse(String query, String rate) {
        return String.format("[%s]", jsonResponse(query, rate));
    }

    private String jsonResponse(String query, String rate) {
        return String.format("{\"target\": \"%s\", \"datapoints\": [[%s, %s]]}", query, rate, new Date().getTime());
    }
}
