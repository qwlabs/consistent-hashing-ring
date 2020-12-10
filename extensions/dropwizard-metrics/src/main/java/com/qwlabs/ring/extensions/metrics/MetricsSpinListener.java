package com.qwlabs.ring.extensions.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.qwlabs.ring.SpinListener;

public class MetricsSpinListener implements SpinListener {
    private final MetricRegistry metricRegistry;
    private final String name;
    private Timer timer;

    public MetricsSpinListener(MetricRegistry metricRegistry, String name) {
        this.metricRegistry = metricRegistry;
        this.name = name;
    }

    @Override
    public void onStart() {
        timer = metricRegistry.timer(name);
    }

    @Override
    public void onOutOfRetryTimes() {

    }

    @Override
    public void onStop() {

    }
}
