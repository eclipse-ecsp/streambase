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

package org.eclipse.ecsp.stream.dma.handler;

import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntry;
import org.springframework.stereotype.Component;

import java.util.List;


/**
 * This is just Dummy implementation of FilterDMOfflineBufferEntry.
 *
 * @author JDEHURY
 */

@Component
public class TestFilterDMOfflineBufferEntryImpl implements FilterDMOfflineBufferEntry {

    /**
     * Filter and update dm offline buffer entries.
     *
     * @param bufferedEntries the buffered entries
     * @return the list
     */
    @Override
    public List<DMOfflineBufferEntry> filterAndUpdateDmOfflineBufferEntries(List<DMOfflineBufferEntry> 
        bufferedEntries) {
        // This is dummy implementation to filter the list. So removing an entry
        // from the list for test.
        if (bufferedEntries.size() > 1) {
            bufferedEntries.remove(0);
        }
        return bufferedEntries;
    }
}
