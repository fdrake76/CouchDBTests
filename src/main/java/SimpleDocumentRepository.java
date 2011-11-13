import org.ektorp.CouchDbConnector;
import org.ektorp.support.CouchDbRepositorySupport;

/**
 * Created by IntelliJ IDEA.
 * User: fdrake
 * Date: 11/12/11
 * Time: 7:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class SimpleDocumentRepository extends CouchDbRepositorySupport<SimpleDocument> {

    public SimpleDocumentRepository(CouchDbConnector db) {
        super(SimpleDocument.class, db);
    }


}
