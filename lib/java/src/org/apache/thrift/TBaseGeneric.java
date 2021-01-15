package org.apache.thrift;

public interface TBaseGeneric<T extends TBaseGeneric<T,F>, F extends TFieldIdEnum> extends Comparable<T>,  TBase {

}
