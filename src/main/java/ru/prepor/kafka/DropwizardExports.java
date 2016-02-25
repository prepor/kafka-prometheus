package ru.prepor.kafka;


import com.yammer.metrics.core.*;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.stats.Snapshot;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Collect dropwizard metrics from a MetricRegistry.
 */
public class DropwizardExports extends io.prometheus.client.Collector {
    private String prefix;
    private MetricsRegistry registry;
    private static final Logger LOGGER = Logger.getLogger(DropwizardExports.class.getName());

    /**
     * @param registry a metric registry to export in prometheus.
     */
    public DropwizardExports(MetricsRegistry registry, String prefix) {
        this.prefix = prefix;
        this.registry = registry;
    }

    /**
     * Export counter as prometheus counter.
     */
    List<MetricFamilySamples> fromCounter(String name, Counter counter) {
        MetricFamilySamples.Sample sample = new MetricFamilySamples.Sample(name, new ArrayList<String>(), new ArrayList<String>(),
                new Long(counter.count()).doubleValue());
        return Arrays.asList(new MetricFamilySamples(name, Type.GAUGE, getHelpMessage(name, counter), Arrays.asList(sample)));
    }

    private static String getHelpMessage(String metricName, Metric metric) {
        return String.format("Generated from dropwizard metric import (metric=%s, type=%s)",
                metricName, metric.getClass().getName());
    }

    /**
     * Export gauge as a prometheus gauge.
     */
    List<MetricFamilySamples> fromGauge(String name, Gauge gauge) {
        Object obj = gauge.value();
        Double value;
        if (obj instanceof Integer) {
            value = Double.valueOf(((Integer) obj).doubleValue());
        } else if (obj instanceof Double) {
            value = (Double) obj;
        } else if (obj instanceof Float) {
            value = Double.valueOf(((Float) obj).doubleValue());
        } else if (obj instanceof Long) {
            value = Double.valueOf(((Long) obj).doubleValue());
        } else {
            LOGGER.log(Level.FINE, String.format("Invalid type for Gauge %s: %s", name,
                    obj.getClass().getName()));
            return new ArrayList<MetricFamilySamples>();
        }
        MetricFamilySamples.Sample sample = new MetricFamilySamples.Sample(name,
                new ArrayList<String>(), new ArrayList<String>(), value);
        return Arrays.asList(new MetricFamilySamples(name, Type.GAUGE, getHelpMessage(name, gauge), Arrays.asList(sample)));
    }

    /**
     * Export a histogram snapshot as a prometheus SUMMARY.
     *
     * @param name     metric name.
     * @param snapshot the histogram snapshot.
     * @param count    the total sample count for this snapshot.
     * @param factor   a factor to apply to histogram values.
     */
    List<MetricFamilySamples> fromSnapshotAndCount(String name, Snapshot snapshot, long count, double factor, String helpMessage) {
        double sum = 0;
        for (double i : snapshot.getValues()) {
            sum += i;
        }
        List<MetricFamilySamples.Sample> samples = Arrays.asList(
                new MetricFamilySamples.Sample(name, Arrays.asList("quantile"), Arrays.asList("0.5"), snapshot.getMedian() * factor),
                new MetricFamilySamples.Sample(name, Arrays.asList("quantile"), Arrays.asList("0.75"), snapshot.get75thPercentile() * factor),
                new MetricFamilySamples.Sample(name, Arrays.asList("quantile"), Arrays.asList("0.95"), snapshot.get95thPercentile() * factor),
                new MetricFamilySamples.Sample(name, Arrays.asList("quantile"), Arrays.asList("0.98"), snapshot.get98thPercentile() * factor),
                new MetricFamilySamples.Sample(name, Arrays.asList("quantile"), Arrays.asList("0.99"), snapshot.get99thPercentile() * factor),
                new MetricFamilySamples.Sample(name, Arrays.asList("quantile"), Arrays.asList("0.999"), snapshot.get999thPercentile() * factor),
                new MetricFamilySamples.Sample(name + "_count", new ArrayList<String>(), new ArrayList<String>(), count),
                new MetricFamilySamples.Sample(name + "_sum", new ArrayList<String>(), new ArrayList<String>(), sum * factor)
        );
        return Arrays.asList(
                new MetricFamilySamples(name, Type.SUMMARY, helpMessage, samples)
        );
    }

    /**
     * Convert histogram snapshot.
     */
    List<MetricFamilySamples> fromHistogram(String name, Histogram histogram) {
        return fromSnapshotAndCount(name, histogram.getSnapshot(), histogram.count(), 1.0,
                getHelpMessage(name, histogram));
    }

    /**
     * Export dropwizard Timer as a histogram. Use TIME_UNIT as time unit.
     */
    List<MetricFamilySamples> fromTimer(String name, Timer timer) {
        return fromSnapshotAndCount(name, timer.getSnapshot(), timer.count(),
                1.0D / TimeUnit.SECONDS.toNanos(1L), getHelpMessage(name, timer));
    }

    /**
     * Export a Meter as as prometheus COUNTER.
     */
    List<MetricFamilySamples> fromMeter(String name, Meter meter) {
        return Arrays.asList(
                new MetricFamilySamples(name + "_total", Type.COUNTER, getHelpMessage(name, meter),
                        Arrays.asList(new MetricFamilySamples.Sample(name + "_total",
                                new ArrayList<String>(),
                                new ArrayList<String>(),
                                meter.count())))

        );
    }

    /**
     * Replace all unsupported chars with '_'.
     *
     * @param name metric name.
     * @return the sanitized metric name.
     */
    public String sanitizeMetricName(String name) {
        return prefix + name.replaceAll("[^a-zA-Z0-9:_]", "_");
    }

    @Override
    public List<MetricFamilySamples> collect() {
        ArrayList<MetricFamilySamples> mfSamples = new ArrayList<MetricFamilySamples>();
        for (Map.Entry<MetricName, Metric> entry : registry.allMetrics().entrySet()) {
            String name = sanitizeMetricName(entry.getKey().getName());
            Metric metric = entry.getValue();
            List<MetricFamilySamples> samples;
            if (metric instanceof Gauge) {
                samples = fromGauge(name, (Gauge) metric);
            } else if (metric instanceof Counter) {
                samples = fromCounter(name, (Counter) metric);
            } else if (metric instanceof Histogram) {
                samples = fromHistogram(name, (Histogram) metric);
            } else if (metric instanceof Timer) {
                samples = fromTimer(name, (Timer) metric);
            } else if (metric instanceof Meter) {
                samples = fromMeter(name, (Meter) metric);
            } else {
                samples = new LinkedList<>();
            }
            mfSamples.addAll(samples);
        }
        return mfSamples;
    }
}
