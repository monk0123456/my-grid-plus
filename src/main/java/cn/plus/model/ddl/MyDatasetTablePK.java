package cn.plus.model.ddl;

import java.io.Serializable;

public class MyDatasetTablePK implements Serializable {
    private static final long serialVersionUID = 7047886669947617311L;

    private Long id;
    private Long dataset_id;

    public MyDatasetTablePK()
    {}

    public MyDatasetTablePK(final Long id, final Long dataset_id)
    {
        this.id = id;
        this.dataset_id = dataset_id;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getDataset_id() {
        return dataset_id;
    }

    public void setDataset_id(Long dataset_id) {
        this.dataset_id = dataset_id;
    }

    @Override
    public String toString() {
        return "MyDatasetTablePK{" +
                "id=" + id +
                ", dataset_id=" + dataset_id +
                '}';
    }
}
