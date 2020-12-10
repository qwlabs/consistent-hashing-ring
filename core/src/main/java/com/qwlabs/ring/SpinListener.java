package com.qwlabs.ring;

public interface SpinListener {

    void onStart();

    void onStop();

    void onOutOfRetryTimes();
}
