package cn.plus.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

/**
 * 数据集中 ddl 的 log 即 table 的 ddl
 * 更新数据集时，先执行 更新数据集、在更新表、在更新表中的数据
 * */
public class DdlLog implements Serializable {
    private static final long serialVersionUID = 7539826226124617909L;

    private Long id;
    private Long group_id;
    private Long data_set_id;
    private String sql_code;
    private Timestamp create_date;

    public DdlLog(final Long id, final Long group_id, final String sql_code, final Long data_set_id)
    {
        this.id = id;
        this.group_id = group_id;
        this.sql_code = sql_code;
        this.data_set_id = data_set_id;
        this.create_date = new Timestamp((new Date()).getTime());
    }

    public DdlLog()
    {
        this.create_date = new Timestamp((new Date()).getTime());
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getGroup_id() {
        return group_id;
    }

    public void setGroup_id(Long group_id) {
        this.group_id = group_id;
    }

    public Long getData_set_id() {
        return data_set_id;
    }

    public void setData_set_id(Long data_set_id) {
        this.data_set_id = data_set_id;
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
        return "DdlLog{" +
                "id=" + id +
                ", group_id=" + group_id +
                ", data_set_id=" + data_set_id +
                ", sql_code='" + sql_code + '\'' +
                ", create_date=" + create_date +
                '}';
    }
}








































