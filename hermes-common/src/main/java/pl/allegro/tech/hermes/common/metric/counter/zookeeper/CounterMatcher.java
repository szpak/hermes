package pl.allegro.tech.hermes.common.metric.counter.zookeeper;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import pl.allegro.tech.hermes.common.metric.Counters;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static pl.allegro.tech.hermes.common.metric.counter.zookeeper.ZookeeperCounterStorage.normalizedName;

class CounterMatcher {

    private static final int HOSTNAME_INDEX = 2;
    private static final int NORMALIZE_NAME_INDEX = 3;
    private static final int TOPIC_NAME_INDEX = 4;
    private static final int SUBSCRIPTION_NAME_INDEX = 7;

    private static final String DOT = "\\.";
    private static final String SOURCE_NAME = "([^\\.]+)";
    private static final String NORMALIZE_NAME = "([^\\.]+)";
    private static final String QUALIFIED_TOPIC_NAME = "([^\\.]+\\.([^\\.]+))";
    private static final String OPTIONAL_SUBSCRIPTION_NAME = "(\\.?([^\\.]+))?(.*)";
    private static final String HOSTNAME = "(\\Q%s\\E)";

    private final Matcher matcher;
    private final String counterName;

    public CounterMatcher(String counterName, String hostname) {
        this.matcher = createMatcher(counterName, String.format(HOSTNAME, hostname));
        this.counterName = counterName;
    }

    private Matcher createMatcher(String counterName, String hostname) {
        String[] tokens = {SOURCE_NAME, hostname, NORMALIZE_NAME, QUALIFIED_TOPIC_NAME + OPTIONAL_SUBSCRIPTION_NAME};
        return Pattern.compile(Joiner.on(DOT).join(tokens)).matcher(counterName);
    }

    public boolean matches() {
        return matcher.matches();
    }

    public boolean isTopic() {
        return !isSubscription();
    }

    public boolean isSubscription() {
        return Optional.fromNullable(matcher.group(SUBSCRIPTION_NAME_INDEX)).isPresent();
    }

    public boolean isInflight() {
        return matcher.group(NORMALIZE_NAME_INDEX).equals(normalizedName(Counters.CONSUMER_INFLIGHT));
    }

    public String getCounter() {
        return counterName;
    }

    public String getTopicName() {
        return matcher.group(TOPIC_NAME_INDEX);
    }

    public String getSubscriptionName() {
        return matcher.group(SUBSCRIPTION_NAME_INDEX);
    }

    public String getHostname() {
        return matcher.group(HOSTNAME_INDEX);
    }

}
