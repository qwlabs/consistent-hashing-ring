package com.qwlabs.ring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSpinListener implements SpinListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSpinListener.class);

    @Override
    public void onStart() {
        LOGGER.info("start spin while consistent hashing is empty");
    }

    @Override
    public void onStop() {
        LOGGER.info("stop spin while consistent hashing is empty");
    }

    @Override
    public void onOutOfRetryTimes() {
        LOGGER.warn("out of retry times to spin while consistent hashing is empty");
    }
}
