package cn.plus.model.ddl;

import java.io.Serializable;

public class MyViewObj implements Serializable {

    private static final long serialVersionUID = 1232441981577849157L;

    private ViewOperateType operate;
    private ViewType code_type;
    private String view_name;
    private String table_name;
    private String data_set_name;
    private String code;

    public MyViewObj(final ViewOperateType operate, final ViewType code_type, final String view_name, final String table_name, final String data_set_name, final String code)
    {
        this.operate = operate;
        this.code_type = code_type;
        this.view_name = view_name;
        this.table_name = table_name;
        this.data_set_name = data_set_name;
        this.code = code;
    }

    public MyViewObj()
    {}

    public ViewOperateType getOperate() {
        return operate;
    }

    public void setOperate(ViewOperateType operate) {
        this.operate = operate;
    }

    public ViewType getCode_type() {
        return code_type;
    }

    public void setCode_type(ViewType code_type) {
        this.code_type = code_type;
    }

    public String getView_name() {
        return view_name;
    }

    public void setView_name(String view_name) {
        this.view_name = view_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getData_set_name() {
        return data_set_name;
    }

    public void setData_set_name(String data_set_name) {
        this.data_set_name = data_set_name;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    @Override
    public String toString() {
        return "MyViewObj{" +
                "operate=" + operate +
                ", code_type=" + code_type +
                ", view_name='" + view_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", data_set_name='" + data_set_name + '\'' +
                ", code='" + code + '\'' +
                '}';
    }
}
