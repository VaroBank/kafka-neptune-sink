/*
Copyright (c) 2022 Varo Bank, N.A. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package com.varobank.kafka.neptune.utils;

import com.varobank.common.gremlin.utils.RetryCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class RetryClient {

    private static final Random random = new Random();
    private static final Logger logger = LoggerFactory.getLogger(RetryClient.class);

    public int retry(Runnable task, int retryCount, int baseMillis, RetryCondition... conditions) throws Exception {

        int count = 0;
        boolean allowContinue = true;
        Exception lastException = null;


        while (allowContinue) {

            if (count > 0) {
                try {
                    int delayMillis = baseMillis + ((2 ^ count) * 15) + random.nextInt(100);
                    logger.warn(String.format("Retry – count %s delay %s", count, delayMillis));
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    // Do nothing
                }
            }

            try {
                task.run();
                allowContinue = false;
            } catch (Exception e) {
                if (count < retryCount) {
                    boolean isRetriable = false;
                    for (RetryCondition condition : conditions) {
                        try {
                            if (condition.allowRetry(e)) {
                                count++;
                                isRetriable = true;
                                logger.warn(String.format("Retriable exception: %s", e.getMessage()));
                                break;
                            }
                        } catch (Exception ex) {
                            logger.debug(ex.getMessage());
                        }
                    }
                    if (!isRetriable) {
                        lastException = e;
                        allowContinue = false;
                    }
                } else {
                    lastException = e;
                    allowContinue = false;
                }
            }
        }


        if (lastException != null) {
            throw lastException;
        }

        return count;
    }

    public int retry(Runnable task, int retryCount, RetryCondition... conditions) throws Exception {
        return retry(task, retryCount, 500, conditions);
    }
}
