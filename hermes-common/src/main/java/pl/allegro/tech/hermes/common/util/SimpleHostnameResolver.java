package pl.allegro.tech.hermes.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public final class SimpleHostnameResolver implements HostnameResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleHostnameResolver.class);

    public SimpleHostnameResolver() { }

    public String resolve() {
        String hostname = "hostname-could-not-be-detected";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOGGER.warn("Could not determine hostname");
        }
        return hostname;
    }

}
