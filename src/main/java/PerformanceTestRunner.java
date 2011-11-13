import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.ektorp.*;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;

import java.io.*;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: fdrake
 * Date: 11/12/11
 * Time: 11:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class PerformanceTestRunner {
    private String dbUrl1;
    private String dbUrl2;
    private String dbName;
    private String attachmentFilename;
    private String attachmentMimeType;
    private int runsPerTest;
    private String outputFilename;
    private int[] operationCounts;

    private CouchDbInstance dbInstance1;
    private CouchDbInstance dbInstance2;
    private CouchDbConnector db1;
    private CouchDbConnector db2;

    private Operation simpleAddOperation = new Operation(){
        public void preOperation(SimpleDocumentRepository repo, CouchDbConnector db, SimpleDocument doc) {}

        public void operation(SimpleDocumentRepository repo, CouchDbConnector db, SimpleDocument doc) {
            repo.add(doc);
        }
    };

    public PerformanceTestRunner() throws Exception {
        buildProperties();
        new File(outputFilename).delete();
                HttpClient httpClient1 = new StdHttpClient.Builder()
                .url(dbUrl1).build();
        HttpClient httpClient2 = new StdHttpClient.Builder()
                .url(dbUrl2).build();
        dbInstance1 = new StdCouchDbInstance(httpClient1);
        dbInstance2 = new StdCouchDbInstance(httpClient2);
        db1 = new StdCouchDbConnector(dbName, dbInstance1);
        db2 = new StdCouchDbConnector(dbName, dbInstance2);
        db1.createDatabaseIfNotExists();
        db2.createDatabaseIfNotExists();
    }

    public void cleanup() {
        db1.createDatabaseIfNotExists();
        db2.createDatabaseIfNotExists();
        dbInstance1.deleteDatabase(dbName);
        dbInstance2.deleteDatabase(dbName);
    }

    public void runTests() {
        for(int operations : operationCounts) {
            runOperationBatch("DB1 "+operations+" Inserts", simpleAddOperation, dbInstance1, db1, operations);
            runOperationBatch("DB2 "+operations+" Inserts", simpleAddOperation, dbInstance2, db2, operations);
            runOperationBatch("DB1 "+operations+" Inserts With Attachment", attachmentAddOperation, dbInstance1, db1, operations);
            runOperationBatch("DB2 "+operations+" Inserts With Attachment", attachmentAddOperation, dbInstance2, db2, operations);
            runOperationBatch("Round Robin Replication "+operations+" Inserts",
                    addWithMutualContinuousReplication,
                    new CouchDbInstance[]{dbInstance1, dbInstance2},
                    new CouchDbConnector[]{db1, db2}, operations);

        }
    }

    private void buildProperties() {
        Properties props = new Properties();
        try {
            props.load(getClass().getResourceAsStream("testrunner.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        dbUrl1 = props.getProperty("dbUrl1");
        dbUrl2 = props.getProperty("dbUrl2");
        dbName = props.getProperty("dbName");
        attachmentFilename = props.getProperty("attachmentFilename");
        attachmentMimeType = props.getProperty("attachmentMimeType");
        runsPerTest = Integer.parseInt(props.getProperty("runsPerTest"));
        outputFilename = props.getProperty("outputFilename");

        String[] opCountsTokens = props.getProperty("operationCounts").split(",");
        operationCounts = new int[opCountsTokens.length];
        for(int i=0; i<opCountsTokens.length;i++)
            operationCounts[i] = Integer.parseInt(opCountsTokens[i]);
    }

    private void runOperationBatch(String label, Operation operation, CouchDbInstance dbInstance,
                                   CouchDbConnector db, int operationCount) {
        runOperationBatch(label, operation, new CouchDbInstance[]{dbInstance},
                new CouchDbConnector[]{db}, operationCount);
    }

    private void runOperationBatch(String label, Operation operation, CouchDbInstance[] instances,
                                   CouchDbConnector[] connectors, int operations) {
        for(int i=0;i< runsPerTest;i++) {
            for(CouchDbConnector db : connectors)
                db.createDatabaseIfNotExists();
            PerformanceMetric metric = performOperation(operation, connectors, operations);
            printResults(label+" "+(i+1), metric);
            for(CouchDbInstance instance : instances)
                instance.deleteDatabase(dbName);
        }
    }

    private void printHeader() {
        System.out.println("Run Name,Avg,Min,Max,Total");
    }

    private void printResults(String name, PerformanceMetric metric) {
        StringBuilder sb = new StringBuilder();
        sb.append(name).append(",");
        sb.append(metric.getMeanMeasurement());
        sb.append(",").append(metric.getMinMeasurement());
        sb.append(",").append(metric.getMaxMeasurement());
        sb.append(",").append(metric.getTotalTime());
        System.out.println(sb.toString());
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(outputFilename, true));
            out.write(sb.toString()+"\n");
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private SimpleDocument buildDocument() {
        SimpleDocument doc = new SimpleDocument();
        doc.setFirstName("John");
        doc.setLastName("Doe");
        doc.setAddress("11 Mockingbird Ln.");
        doc.setAge(35);
        return doc;
    }

    private PerformanceMetric performOperation(Operation operation, CouchDbConnector[] connectors,
                                               int operationCount) {
        SimpleDocumentRepository[] repos = new SimpleDocumentRepository[connectors.length];
        for(int i=0; i<connectors.length; i++)
            repos[i] = new SimpleDocumentRepository(connectors[i]);

        int roundRobinCounter = 0;
        PerformanceMetric metric = new PerformanceMetric();
        for(int i=0;i<operationCount;i++) {
            SimpleDocument doc = buildDocument();
            operation.preOperation(repos[roundRobinCounter], connectors[roundRobinCounter], doc);
            long startTime = System.currentTimeMillis();
            operation.operation(repos[roundRobinCounter], connectors[roundRobinCounter], doc);
            long endTime = System.currentTimeMillis();
            metric.addMeasurement(endTime - startTime);

            roundRobinCounter++;
            if (roundRobinCounter >= repos.length) roundRobinCounter = 0;
        }

        return metric;
    }


    private Operation attachmentAddOperation = new Operation() {
        String dataB64 = null;

        public void preOperation(SimpleDocumentRepository repo, CouchDbConnector db, SimpleDocument doc) {
            InputStream stream = getClass().getResourceAsStream("/"+ attachmentFilename);
            byte[] data = new byte[0];
            try {
                data = IOUtils.toByteArray(stream);
            } catch (IOException e) {
                e.printStackTrace();
            }

            dataB64 = Base64.encodeBase64String(data);
        }

        public void operation(SimpleDocumentRepository repo, CouchDbConnector db, SimpleDocument doc) {
            Attachment a = new Attachment("picture", dataB64, attachmentMimeType);
            doc.addInlineAttachment(a);
            repo.add(doc);
        }
    };

    private Operation addWithMutualContinuousReplication = new Operation() {

        public void preOperation(SimpleDocumentRepository repo, CouchDbConnector db, SimpleDocument doc) {
            ReplicationCommand cmdFrom1To2 = new ReplicationCommand.Builder()
                    .source(dbName)
                    .target(dbUrl2 +"/"+ dbName)
                    .continuous(true)
                    .build();
            ReplicationStatus status1 = dbInstance1.replicate(cmdFrom1To2);

            ReplicationCommand cmdFrom2To1 = new ReplicationCommand.Builder()
                    .source(dbName)
                    .target(dbUrl1 +"/"+ dbName)
                    .continuous(true)
                    .build();
            ReplicationStatus status2 = dbInstance2.replicate(cmdFrom2To1);
        }

        public void operation(SimpleDocumentRepository repo, CouchDbConnector db, SimpleDocument doc) {
            repo.add(doc);
        }
    };


    private interface Operation {
        void preOperation(SimpleDocumentRepository repo, CouchDbConnector db, SimpleDocument doc);
        void operation(SimpleDocumentRepository repo, CouchDbConnector db, SimpleDocument doc);
    }


    public static void main(String[] args) throws Exception {
        PerformanceTestRunner runner = new PerformanceTestRunner();
        runner.runTests();
        runner.cleanup();
    }
}
