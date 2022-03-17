package cn.plus.model.ddl;

import java.io.Serializable;

public class MyDatasetTable implements Serializable {
    private static final long serialVersionUID = 6553465390741411989L;

    private Long id;
    private String table_name;
    private Long dataset_id;
    private Boolean to_real;

    public MyDatasetTable()
    {}

    public MyDatasetTable(final Long id, final String table_name, final Long dataset_id, final Boolean to_real)
    {
        this.id = id;
        this.table_name = table_name;
        this.dataset_id = dataset_id;
        this.to_real = to_real;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public Long getDataset_id() {
        return dataset_id;
    }

    public void setDataset_id(Long dataset_id) {
        this.dataset_id = dataset_id;
    }

    public Boolean getTo_real() {
        return to_real;
    }

    public void setTo_real(Boolean to_real) {
        this.to_real = to_real;
    }

    @Override
    public String toString() {
        return "MyDatasetTable{" +
                "id=" + id +
                ", table_name='" + table_name + '\'' +
                ", dataset_id=" + dataset_id +
                ", to_real=" + to_real +
                '}';
    }
}
