package org.hill30.activemq.kahadb.reader;

import org.apache.activemq.ActiveMQMessageAuditNoSync;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.kahadb.JournalCommand;
import org.apache.activemq.store.kahadb.data.KahaAckMessageFileMapCommand;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaCommitCommand;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaEntryType;
import org.apache.activemq.store.kahadb.data.KahaPrepareCommand;
import org.apache.activemq.store.kahadb.data.KahaProducerAuditCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveDestinationCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaRollbackCommand;
import org.apache.activemq.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.activemq.store.kahadb.data.KahaTraceCommand;
import org.apache.activemq.store.kahadb.data.KahaUpdateMessageCommand;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.util.DataByteArrayInputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.hill30.activemq.kahadb.utils.KahaDBTransactionIdConversion;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.NotDirectoryException;
import java.util.Map;
import java.util.Set;

import static org.hill30.activemq.Utils.isNullOrEmpty;
import static org.hill30.activemq.Utils.pressAnyKeyToContinue;
import static org.hill30.activemq.Utils.showException;
import static org.hill30.activemq.Utils.showSeparator;
import static org.hill30.activemq.kahadb.utils.KahaDBUtils.createJournal;
import static org.hill30.activemq.kahadb.utils.KahaDBUtils.getDestinationInfo;

public final class KahaDBJournalsReader {
    //region private
    private final boolean showFileMapCommand;
    //-------------------------------------------------------------------------
    private void showData(File sourceDir, boolean useAnyKeyToContinue) throws IOException {
        showJournalData(createJournal(sourceDir), useAnyKeyToContinue);
        //---------------------------------------------------------------------
        for (File sourceSubDir : sourceDir.listFiles()) {
            if(sourceSubDir.isDirectory()) {
                if(useAnyKeyToContinue) {
                    showSeparator();
                    pressAnyKeyToContinue();
                }
                showData(sourceSubDir, useAnyKeyToContinue);
            }
        }
        //---------------------------------------------------------------------
    }
    //-------------------------------------------------------------------------
    private void showJournalData(Journal journal, boolean useAnyKeyToContinue) {
        try {
            showSeparator();

            journal.start();

            int fileIndex = 0;
            int dataIndex = 0;
            File lastFile = null;

            Location location = journal.getNextLocation(null);
            while (location != null) {
                File nextFile = journal.getFile(location.getDataFileId());
                if(lastFile == null || !lastFile.equals(nextFile)) {
                    if(lastFile != null) {
                        showSeparator();
                    }

                    if(useAnyKeyToContinue && lastFile != null) {
                        pressAnyKeyToContinue();
                        showSeparator();
                    }

                    lastFile = nextFile;
                    dataIndex = 1;
                    ++fileIndex;

                    System.out.printf("(%s) Journal: '%s'.\r\n", fileIndex, lastFile);
                    System.out.println();
                }
                else {
                    ++dataIndex;
                }

                ByteSequence sequence = journal.read(location);
                showJournalData(sequence, dataIndex);

                location = journal.getNextLocation(location);
            }

            if(lastFile != null) {
                showSeparator();
            }
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
        finally {
            try {
                journal.close();
            }
            catch (IOException e) {}
        }
    }
    private void showJournalData(ByteSequence sequence, int dataIndex) throws IOException {
        DataByteArrayInputStream sequenceDataStream = new DataByteArrayInputStream(sequence);
        KahaEntryType commandType = KahaEntryType.valueOf(sequenceDataStream.readByte());

        JournalCommand<?> command = (JournalCommand<?>)commandType.createMessage();
        command.mergeFramed(sequenceDataStream);
        //---------------------------------------------------------------------
        if(showCommandAvailable(commandType)) {
            String info = getCommandInfo(commandType, command);
            String commandTypeStr = isReversCommand(commandType, command) ? "-" + commandType.toString() : commandType.toString();
            if (info == null || info.isEmpty()) {
                System.out.printf("%s CommandType: %s.\r\n", dataIndex, commandTypeStr);
            }
            else {
                System.out.printf("%s CommandType: %s - %s.\r\n", dataIndex, commandTypeStr, info);
            }
        }
    }
    //-------------------------------------------------------------------------
    private boolean showCommandAvailable(KahaEntryType commandType) {
        if(commandType == KahaEntryType.KAHA_ACK_MESSAGE_FILE_MAP_COMMAND) {
            return showFileMapCommand;
        }
        return true;
    }
    private boolean isReversCommand(KahaEntryType commandType, JournalCommand<?> command) {
        if(commandType == KahaEntryType.KAHA_SUBSCRIPTION_COMMAND) {
            KahaSubscriptionCommand subscriptionCommand = (KahaSubscriptionCommand)command;
            return !subscriptionCommand.hasSubscriptionInfo();
        }
        return false;
    }
    //-------------------------------------------------------------------------
    private String getCommandInfo(KahaEntryType commandType, JournalCommand<?> command) {
        switch (commandType) {
            case KAHA_ADD_MESSAGE_COMMAND: {
                return getCommandInfo((KahaAddMessageCommand)command);
            }
            case KAHA_UPDATE_MESSAGE_COMMAND: {
                return getCommandInfo((KahaUpdateMessageCommand)command);
            }
            case KAHA_REMOVE_MESSAGE_COMMAND: {
                return getCommandInfo((KahaRemoveMessageCommand)command);
            }
            case KAHA_REMOVE_DESTINATION_COMMAND: {
                return getCommandInfo((KahaRemoveDestinationCommand)command);
            }
            case KAHA_SUBSCRIPTION_COMMAND: {
                return getCommandInfo((KahaSubscriptionCommand)command);
            }
            case KAHA_ACK_MESSAGE_FILE_MAP_COMMAND : {
                return getCommandInfo((KahaAckMessageFileMapCommand)command);
            }
            case KAHA_PRODUCER_AUDIT_COMMAND: {
                return getCommandInfo((KahaProducerAuditCommand)command);
            }
            case KAHA_TRACE_COMMAND: {
                return getCommandInfo((KahaTraceCommand)command);
            }
            case KAHA_COMMIT_COMMAND: {
                return getCommandInfo((KahaCommitCommand)command);
            }
            case KAHA_PREPARE_COMMAND: {
                return getCommandInfo((KahaPrepareCommand)command);
            }
            case KAHA_ROLLBACK_COMMAND: {
                return getCommandInfo((KahaRollbackCommand)command);
            }
            default: {
                return "";
            }
        }
    }

    private String getCommandInfo(KahaAddMessageCommand command) {
        String info = getDestinationInfo(command.getDestination()) + ", MsgId: " + command.getMessageId();
        if(command.hasTransactionInfo()) {
            info += ", TransId: " + KahaDBTransactionIdConversion.convert(command.getTransactionInfo());
        }
        return info;
    }
    private String getCommandInfo(KahaUpdateMessageCommand command) {
        return getCommandInfo(command.getMessage());
    }
    private String getCommandInfo(KahaRemoveMessageCommand command) {
        String info =  getDestinationInfo(command.getDestination()) + ", MsgId: " + command.getMessageId();
        if(command.hasTransactionInfo()) {
            info += ", TransId: " + KahaDBTransactionIdConversion.convert(command.getTransactionInfo());
        }
        if(command.hasSubscriptionKey()) {
           info += ", SubKey: " + command.getSubscriptionKey();
        }
        return info;
    }
    private String getCommandInfo(KahaRemoveDestinationCommand command) {
        return getDestinationInfo(command.getDestination());
    }
    private String getCommandInfo(KahaSubscriptionCommand command) {
        KahaDestination destination = command.getDestination();
        String result = getDestinationInfo(destination);

        if(command.hasSubscriptionInfo()) {
            try {
                Buffer buffer = command.getSubscriptionInfo();
                ByteSequence sequence = new ByteSequence(buffer.getData(), buffer.getOffset(), buffer.getLength());
                WireFormat wireFormat = new OpenWireFormat();
                SubscriptionInfo subscriptionInfo = (SubscriptionInfo)wireFormat.unmarshal(sequence);

                result += ", ClientId: " + subscriptionInfo.getClientId();
            } catch (Throwable throwable) {
                showException(throwable);
            }
        }

        result += ", SubKey: " + command.getSubscriptionKey();

        return result;
    }
    private String getCommandInfo(KahaAckMessageFileMapCommand command) {
        try {
            ObjectInputStream objectIn = new ObjectInputStream(command.getAckMessageFileMap().newInput());
            Map<Integer, Set<Integer>> ackMessageFileMap = (Map<Integer, Set<Integer>>) objectIn.readObject();
            return ackMessageFileMap.toString();
        } catch (Throwable throwable) {
            showException(throwable);
        }
        return "";
    }
    private String getCommandInfo(KahaProducerAuditCommand command) {
        try {
            ObjectInputStream objectIn = new ObjectInputStream(command.getAudit().newInput());
            ActiveMQMessageAuditNoSync producerSequenceIdTracker = (ActiveMQMessageAuditNoSync)objectIn.readObject();
            return "MaxNumProducers: " + producerSequenceIdTracker.getMaximumNumberOfProducersToTrack() + ", MaxAuditDepth: " + producerSequenceIdTracker.getAuditDepth();
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
        return "";
    }
    private String getCommandInfo(KahaTraceCommand command) {
        return command.getMessage();
    }
    private String getCommandInfo(KahaCommitCommand command) {
        if(command.hasTransactionInfo()) {
            return "TransId: " + KahaDBTransactionIdConversion.convert(command.getTransactionInfo());
        }
        return "";
    }
    private String getCommandInfo(KahaPrepareCommand command) {
        if(command.hasTransactionInfo()) {
            return "TransId: " + KahaDBTransactionIdConversion.convert(command.getTransactionInfo());
        }
        return "";
    }
    private String getCommandInfo(KahaRollbackCommand command) {
        if(command.hasTransactionInfo()) {
            return "TransId: " + KahaDBTransactionIdConversion.convert(command.getTransactionInfo());
        }
        return "";
    }
    //endregion
    public KahaDBJournalsReader(boolean showFileMapCommand) {
        this.showFileMapCommand = showFileMapCommand;
    }

    //-------------------------------------------------------------------------
    public void showData(String sourceDirPath, boolean useAnyKeyToContinue) throws NotDirectoryException {
        if(isNullOrEmpty(sourceDirPath)) {
            throw new NullPointerException("sourceDirPath");
        }

        File sourceDir = new File(sourceDirPath);
        if(!sourceDir.isDirectory()) {
            throw new NotDirectoryException("sourceDirPath");
        }

        try {
            showData(sourceDir, useAnyKeyToContinue);
        }
        catch (Throwable throwable) {
            showException(throwable);
        }
    }
    //-------------------------------------------------------------------------
}
