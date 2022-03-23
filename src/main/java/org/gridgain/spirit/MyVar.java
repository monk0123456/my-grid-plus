package org.gridgain.spirit;

import java.io.Serializable;

/**
 * spirit sql 里面的对变量的赋值
 * */
public class MyVar implements Serializable {
    private static final long serialVersionUID = 5645425225182146226L;
    private Object var;
    private String varType;

    public MyVar()
    {}

    public MyVar(final Object var)
    {
        this.var = var;
    }

    public Object getVar() {
        return var;
    }

    public void setVar(Object var) {
        this.var = var;
    }

    public String getVarType() {
        return varType;
    }

    public void setVarType(String varType) {
        this.varType = varType;
    }
}
