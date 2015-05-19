package pl.allegro.tech.hermes.common.di.factories;

import org.glassfish.hk2.api.Factory;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.metric.PathsCompiler;
import pl.allegro.tech.hermes.common.metric.rate.graphite.GraphiteQueries;

import javax.inject.Inject;

public class GraphiteQueriesFactory implements Factory<GraphiteQueries> {

    private final PathsCompiler pathsCompiler;
    private final String graphitePrefix;

    @Inject
    public GraphiteQueriesFactory(PathsCompiler pathsCompiler, ConfigFactory configFactory) {
        this.pathsCompiler = pathsCompiler;
        this.graphitePrefix = configFactory.getStringProperty(Configs.GRAPHITE_PREFIX);
    }

    @Override
    public GraphiteQueries provide() {
        return new GraphiteQueries(graphitePrefix, pathsCompiler);
    }

    @Override
    public void dispose(GraphiteQueries instance) {

    }
}
