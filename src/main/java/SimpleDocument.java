import org.ektorp.support.CouchDbDocument;

/**
 * Created by IntelliJ IDEA.
 * User: fdrake
 * Date: 11/12/11
 * Time: 6:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class SimpleDocument extends CouchDbDocument {
    private String firstName;
    private String lastName;
    private String address;
    private int age;

    public void addInlineAttachment(org.ektorp.Attachment a) {
        super.addInlineAttachment(a);
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
