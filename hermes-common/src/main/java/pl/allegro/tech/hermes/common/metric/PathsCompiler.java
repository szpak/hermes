package pl.allegro.tech.hermes.common.metric;

import org.apache.commons.lang.text.StrBuilder;

import static pl.allegro.tech.hermes.common.metric.HermesMetrics.escapeDots;

public class PathsCompiler {

    public static final String HOSTNAME = "$hostname";
    public static final String GROUP = "$group";
    public static final String TOPIC = "$topic";
    public static final String SUBSCRIPTION = "$subscription";
    public static final String PARTITION = "$partition";
    public static final String HTTP_CODE = "$http_code";
    public static final String EXECUTOR_NAME = "$executor_name";

    private final String hostname;

    public PathsCompiler(String hostname) {
        this.hostname = escapeDots(hostname);
    }

    public String compile(String path) {
        return path.replace(HOSTNAME, hostname);
    }

    public String compile(String path, PathContext context) {
        StrBuilder pathBuilder = new StrBuilder(path);

        context.getGroup().ifPresent(g -> pathBuilder.replaceAll(GROUP, g));
        context.getTopic().ifPresent(t -> pathBuilder.replaceAll(TOPIC, t));
        context.getSubscription().ifPresent(s -> pathBuilder.replaceAll(SUBSCRIPTION, s));
        context.getPartition().ifPresent(p -> pathBuilder.replaceAll(PARTITION, p.toString()));
        context.getHttpCode().ifPresent(c -> pathBuilder.replaceAll(HTTP_CODE, c.toString()));
        context.getExecutorName().ifPresent(c -> pathBuilder.replaceAll(EXECUTOR_NAME, c.toString()));

        pathBuilder.replaceAll(HOSTNAME, hostname);

        return pathBuilder.toString();
    }
}
