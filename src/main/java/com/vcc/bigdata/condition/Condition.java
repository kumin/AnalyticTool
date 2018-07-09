package com.vcc.bigdata.condition;

/**
 * @author: kumin on 02/07/2018
 **/

public interface Condition<T> {
    public T generateQuery();
}
