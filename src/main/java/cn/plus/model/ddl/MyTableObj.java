package cn.plus.model.ddl;

import java.io.Serializable;
import java.util.List;

public class MyTableObj implements Serializable {
    private static final long serialVersionUID = -4031805976353960724L;

    private MyTable myTable;
    private List<MyTableItem> myTableItems;

    public MyTableObj(final MyTable myTable, final List<MyTableItem> myTableItems)
    {
        this.myTable = myTable;
        this.myTableItems = myTableItems;
    }

    public MyTableObj()
    {}

    public MyTable getMyTable() {
        return myTable;
    }

    public void setMyTable(MyTable myTable) {
        this.myTable = myTable;
    }

    public List<MyTableItem> getMyTableItems() {
        return myTableItems;
    }

    public void setMyTableItems(List<MyTableItem> myTableItems) {
        this.myTableItems = myTableItems;
    }

    @Override
    public String toString() {
        return "MyTableObj{" +
                "myTable=" + myTable +
                ", myTableItems=" + myTableItems +
                '}';
    }
}
