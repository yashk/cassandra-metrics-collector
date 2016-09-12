package org.wikimedia.cassandra.metrics.service;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.wikimedia.cassandra.metrics.FilterConfig;

import java.io.IOException;


/**
 * Created by ykochare on 9/9/16.
 */
@Configuration
@EnableAutoConfiguration
@EnableConfigurationProperties(Config.class)
@ConfigurationProperties(prefix="config")
@Component
public class Config {
    private int interval;
    private Jmx jmx;
    private Carbon carbon;
    private FilterConfig filterConfig;


    public Config() {
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public Jmx getJmx() {
        return jmx;
    }

    public void setJmx(Jmx jmx) {
        this.jmx = jmx;
    }

    public Carbon getCarbon() {
        return carbon;
    }

    public void setCarbon(Carbon carbon) {
        this.carbon = carbon;
    }

    public FilterConfig getFilterConfig() {
        return filterConfig;
    }

    public void setFilterConfig(FilterConfig filterConfig) {
        this.filterConfig = filterConfig;
    }


    @ConfigurationProperties(prefix="config.jmx")
    public static class Jmx{
        private String host;
        private int port;

        public Jmx() {
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        @Override
        public String toString() {
            return "Jmx{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    '}';
        }
    }


    @ConfigurationProperties(prefix="config.carbon")
    public static class Carbon{
        private String host;
        private int port;
        private String prefix;

        public Carbon() {
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getPrefix() {
            return prefix;
        }

        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public String toString() {
            return "Carbon{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    ", prefix='" + prefix + '\'' +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "Config{" +
                "interval=" + interval +
                ", jmx=" + jmx +
                ", carbon=" + carbon +
                ", filterConfig=" + filterConfig +
                '}';
    }

}
