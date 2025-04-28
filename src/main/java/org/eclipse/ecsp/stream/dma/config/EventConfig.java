/*
 *
 *
 *   ******************************************************************************
 *
 *    Copyright (c) 2023-24 Harman International
 *
 *
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *
 *    you may not use this file except in compliance with the License.
 *
 *    You may obtain a copy of the License at
 *
 *
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *    Unless required by applicable law or agreed to in writing, software
 *
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *    See the License for the specific language governing permissions and
 *
 *    limitations under the License.
 *
 *
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    *******************************************************************************
 *
 *
 */

package org.eclipse.ecsp.stream.dma.config;


/**
 * To make use of one of the use cases in DMA, which is to keep retrying events until TTL
 * set on them expires even when the retry attempts are exhausted, service must
 * implement this interface to enable this use case.
 *
 * @author hbadshah
 */
public interface EventConfig {
    
    /**
     * Fallback to TTL on max retry exhausted.
     *
     * @return true, if successful
     */
    /*
     * @return boolean: signaling whether to enable this use case or not.
     */
    public boolean fallbackToTTLOnMaxRetryExhausted();
}
