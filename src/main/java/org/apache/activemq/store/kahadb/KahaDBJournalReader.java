/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.kahadb;

import org.apache.activemq.store.kahadb.data.KahaEntryType;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.util.ByteSequence;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Map;
import java.util.TreeMap;

public class KahaDBJournalReader {
    //region private
    private static final int _JornalFileMaxLength = 32 * 1024 * 1024; //32 МБ
    private static final int _BatchSize = 1024;
    private static final String _ShowSeparator = "--------------------------------------------------------------------------------";
    //-------------------------------------------------------------------------
    private final Map<String, JournalStatisticInfo> _journalStatistics = new TreeMap<>();
    private final Journal _journal;
    //-------------------------------------------------------------------------
    private class JournalStatisticInfo {
        //region private
        private final String _fileName;
        private final Map<KahaEntryType, JournalCommandStatisticInfo> _commandStatistics = new TreeMap<>();
        //endregion
        public JournalStatisticInfo(String fileName) {
            _fileName = fileName;
        }

        //---------------------------------------------------------------------
        public String GetFileName() {
            return _fileName;
        }
        public Map<KahaEntryType, JournalCommandStatisticInfo> GetCommandStatistics () {
            return _commandStatistics;
        }
        //---------------------------------------------------------------------
        public void SetSequence(ByteSequence sequence)
        {
            if(sequence == null) {
                throw new NullPointerException("sequence");
            }

            KahaEntryType commandType = KahaEntryType.valueOf(sequence.data[0]);
            if(!_commandStatistics.containsKey(commandType)) {
                _commandStatistics.put(commandType, new JournalCommandStatisticInfo(commandType));
            }

            JournalCommandStatisticInfo commandStatistic = _commandStatistics.get(commandType);
            commandStatistic.SetSequence(sequence);
        }
        //---------------------------------------------------------------------
    }
    private class JournalCommandStatisticInfo {
        //region private
        private final KahaEntryType _commandType;
        //---------------------------------------------------------------------
        private int _count;
        private long _size;
        //endregion
        public JournalCommandStatisticInfo(KahaEntryType commandType) {
            _commandType = commandType;
        }

        //---------------------------------------------------------------------
        public KahaEntryType GetType() {
            return _commandType;
        }
        public int GetCount () {
            return _count;
        }
        public long GetSize() {
            return _size;
        }
        //---------------------------------------------------------------------
        public void SetSequence(ByteSequence sequence) {
            ++_count;
            _size += sequence.getLength();
        }
        //---------------------------------------------------------------------
    }
    //-------------------------------------------------------------------------
    private void _parseJournalLocation(Location journalLocation) throws IOException {
        String journalFileName = _journal.getFile(journalLocation.getDataFileId()).getName();

        if(!_journalStatistics.containsKey(journalFileName)) {
            _journalStatistics.put(journalFileName, new JournalStatisticInfo(journalFileName));
            System.out.println("File reading: " + journalFileName);
        }

        JournalStatisticInfo journalStatisticInfo = _journalStatistics.get(journalFileName);

        ByteSequence sequence = _journal.read(journalLocation);
        journalStatisticInfo.SetSequence(sequence);
    }
    //-------------------------------------------------------------------------
    private void _showStatistics() {
        System.out.println();
        System.out.println(_ShowSeparator);
        System.out.println("JOURNALS STATISTICS");
        System.out.println(_ShowSeparator);

        for (String key: _journalStatistics.keySet()) {
            JournalStatisticInfo journalStatistic = _journalStatistics.get(key);
            Map<KahaEntryType, JournalCommandStatisticInfo> journalCommandStatistics = journalStatistic.GetCommandStatistics();

            System.out.println("FileName: " + journalStatistic.GetFileName());
            System.out.println();
            for (KahaEntryType commandType: journalCommandStatistics.keySet()) {
                JournalCommandStatisticInfo journalCommandStatistic = journalCommandStatistics.get(commandType);
                System.out.println("CommandType: " + journalCommandStatistic.GetType() + " (Count: " + journalCommandStatistic.GetCount() + ", Size: " + _BytesToString(journalCommandStatistic.GetSize()) + ")");
            }
            System.out.println(_ShowSeparator);
        }
    }
    //-------------------------------------------------------------------------
    private static String _BytesToString(long sizeInBytes) {
        final double SPACE_KB = 1024;
        final double SPACE_MB = 1024 * SPACE_KB;
        final double SPACE_GB = 1024 * SPACE_MB;
        final double SPACE_TB = 1024 * SPACE_GB;

        NumberFormat nf = new DecimalFormat();
        nf.setMaximumFractionDigits(2);
        try {
            if (sizeInBytes < SPACE_KB) {
                return nf.format(sizeInBytes) + " Byte(s)";
            } else if (sizeInBytes < SPACE_MB) {
                return nf.format(sizeInBytes / SPACE_KB) + " KB";
            } else if (sizeInBytes < SPACE_GB) {
                return nf.format(sizeInBytes / SPACE_MB) + " MB";
            } else if (sizeInBytes < SPACE_TB) {
                return nf.format(sizeInBytes / SPACE_GB) + " GB";
            } else {
                return nf.format(sizeInBytes / SPACE_TB) + " TB";
            }
        } catch (Exception e) {
            return sizeInBytes + " Byte(s)";
        }
    }
    //endregion
    //-------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        if(args.length != 1) {
            System.out.println("usage JournalReader <journal data directory>");
            System.exit(1);
        }

        //---------------------------------------------------------------------
        KahaDBJournalReader reader = new KahaDBJournalReader(args[0]);
        reader.Parse();
        reader.ShowStatistics();
    }
    //-------------------------------------------------------------------------
    public KahaDBJournalReader(String journalDirPathName) throws Exception {
        if(journalDirPathName == null || journalDirPathName.length() == 0) {
            throw new NullPointerException("journalDirPathName");
        }

        File journalDir = new File(journalDirPathName);
        if(!journalDir.isDirectory()) {
            throw new IllegalArgumentException("journalDirPathName");
        }

        _journal = new Journal();
        _journal.setDirectory(journalDir);
        _journal.setMaxFileLength(_JornalFileMaxLength);
        _journal.setCheckForCorruptionOnStartup(false);
        _journal.setChecksum(false);
        _journal.setWriteBatchSize(_BatchSize);
        _journal.setArchiveDataLogs(false);
    }

    //-------------------------------------------------------------------------
    public void Parse() throws IOException {
        _journal.start();

        Location location = _journal.getNextLocation(null);
        while (location != null) {
            _parseJournalLocation(location);
            location = _journal.getNextLocation(location);
        }
    }
    //-------------------------------------------------------------------------
    public void ShowStatistics() {
        _showStatistics();
    }
    //-------------------------------------------------------------------------
}
