package cn.plus.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

/**
 * 数据集的 ddl log 即 data set 的 ddl
 * 更新数据集时，先执行 更新数据集、在更新表、在更新表中的数据
 * */
public class DataSetDdlLog implements Serializable {
    private static final long serialVersionUID = -7331785681581310787L;

    private Long id;
    private String data_set_name;
    private String ds_ddl_type;
    private String sql_code;
    private Timestamp create_date;

    public DataSetDdlLog(final Long id, final String data_set_name, final String ds_ddl_type, final String sql_code)
    {
        this.id = id;
        this.data_set_name = data_set_name;
        this.ds_ddl_type = ds_ddl_type;
        this.sql_code = sql_code;
        this.create_date = new Timestamp((new Date()).getTime());
    }

    public DataSetDdlLog(final Long id, final String data_set_name, final String ds_ddl_type, final String sql_code, final Timestamp create_date)
    {
        this.id = id;
        this.data_set_name = data_set_name;
        this.ds_ddl_type = ds_ddl_type;
        this.sql_code = sql_code;
        this.create_date = create_date;
    }

    public DataSetDdlLog()
    {}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getData_set_name() {
        return data_set_name;
    }

    public void setData_set_name(String data_set_name) {
        this.data_set_name = data_set_name;
    }

    public String getDs_ddl_type() {
        return ds_ddl_type;
    }

    public void setDs_ddl_type(String ds_ddl_type) {
        this.ds_ddl_type = ds_ddl_type;
    }

    public String getSql_code() {
        return sql_code;
    }

    public void setSql_code(String sql_code) {
        this.sql_code = sql_code;
    }

    public Timestamp getCreate_date() {
        return create_date;
    }

    public void setCreate_date(Timestamp create_date) {
        this.create_date = create_date;
    }

    @Override
    public String toString() {
        return "DataSetDdlLog{" +
                "id=" + id +
                ", data_set_name='" + data_set_name + '\'' +
                ", ds_ddl_type='" + ds_ddl_type + '\'' +
                ", sql_code='" + sql_code + '\'' +
                ", create_date=" + create_date +
                '}';
    }
}


























































































