import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.ektorp.*;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: fdrake
 * Date: 11/12/11
 * Time: 11:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestRunner {
    private static final String DB_URL_1 = "http://192.168.208.101:5984";
    private static final String DB_URL_2 = "http://192.168.208.2:5984";
    private static final String DB_NAME = "test";
    private static final String ATTACHMENT_FILENAME = "picture.jpg";
    private static final String ATTACHMENT_MIME_TYPE = "image/jpeg";
    private static final int RUNS_PER_TEST = 20;
    private static final String OUTPUT_FILENAME = "/tmp/couchdb-tests.csv";
    private static final int[] OPERATION_COUNTS = new int[]{ 50, 100, 250, 500, 1000 };

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

    public TestRunner() throws Exception {
        new File(OUTPUT_FILENAME).delete();
                HttpClient httpClient1 = new StdHttpClient.Builder()
                .url(DB_URL_1).build();
        HttpClient httpClient2 = new StdHttpClient.Builder()
                .url(DB_URL_2).build();
        dbInstance1 = new StdCouchDbInstance(httpClient1);
        dbInstance2 = new StdCouchDbInstance(httpClient2);
        db1 = new StdCouchDbConnector(DB_NAME, dbInstance1);
        db2 = new StdCouchDbConnector(DB_NAME, dbInstance2);
        db1.createDatabaseIfNotExists();
        db2.createDatabaseIfNotExists();
    }

    public void cleanup() {
        db1.createDatabaseIfNotExists();
        db2.createDatabaseIfNotExists();
        dbInstance1.deleteDatabase(DB_NAME);
        dbInstance2.deleteDatabase(DB_NAME);
    }

    public void runTests() {
        for(int operations : OPERATION_COUNTS) {
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

    private void runOperationBatch(String label, Operation operation, CouchDbInstance dbInstance,
                                   CouchDbConnector db, int operationCount) {
        runOperationBatch(label, operation, new CouchDbInstance[]{dbInstance},
                new CouchDbConnector[]{db}, operationCount);
    }

    private void runOperationBatch(String label, Operation operation, CouchDbInstance[] instances,
                                   CouchDbConnector[] connectors, int operations) {
        for(int i=0;i<RUNS_PER_TEST;i++) {
            for(CouchDbConnector db : connectors)
                db.createDatabaseIfNotExists();
            PerformanceMetric metric = performOperation(operation, connectors, operations);
            printResults(label+" "+(i+1), metric);
            for(CouchDbInstance instance : instances)
                instance.deleteDatabase(DB_NAME);
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
            BufferedWriter out = new BufferedWriter(new FileWriter(OUTPUT_FILENAME, true));
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
            InputStream stream = getClass().getResourceAsStream("/"+ATTACHMENT_FILENAME);
            byte[] data = new byte[0];
            try {
                data = IOUtils.toByteArray(stream);
            } catch (IOException e) {
                e.printStackTrace();
            }

            dataB64 = Base64.encodeBase64String(data);
        }

        public void operation(SimpleDocumentRepository repo, CouchDbConnector db, SimpleDocument doc) {
            Attachment a = new Attachment("picture", dataB64, ATTACHMENT_MIME_TYPE);
            doc.addInlineAttachment(a);
            repo.add(doc);
        }
    };

    private Operation addWithMutualContinuousReplication = new Operation() {

        public void preOperation(SimpleDocumentRepository repo, CouchDbConnector db, SimpleDocument doc) {
            ReplicationCommand cmdFrom1To2 = new ReplicationCommand.Builder()
                    .source(DB_NAME)
                    .target(DB_URL_2+"/"+DB_NAME)
                    .continuous(true)
                    .build();
            ReplicationStatus status1 = dbInstance1.replicate(cmdFrom1To2);

            ReplicationCommand cmdFrom2To1 = new ReplicationCommand.Builder()
                    .source(DB_NAME)
                    .target(DB_URL_1+"/"+DB_NAME)
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
        TestRunner runner = new TestRunner();
        runner.runTests();
        runner.cleanup();
    }
}
