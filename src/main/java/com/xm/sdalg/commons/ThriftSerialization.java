package com.xm.sdalg.commons;

import org.apache.log4j.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

public class ThriftSerialization {
    private final static Logger logger = Logger.getLogger(ThriftSerialization.class);


    public static <T extends TBase> byte[] toBytes(T t){
        return toBytes(t, SerializationProtocol.BINARAY.getTProtocolFactory());
    }
    public static <T extends TBase> String toJson(T t){
        return new String(toBytes(t, SerializationProtocol.JSON.getTProtocolFactory()));
    }
    public  static  <T extends TBase> T fromBytes(T t,byte[] bytes){
        return fromBytes(t,bytes, SerializationProtocol.BINARAY.getTProtocolFactory());
    }
    public static <T extends TBase> T fromBytes(Class<T> tClass,byte[] bytes){
        return fromBytes(tClass, bytes, SerializationProtocol.BINARAY.getTProtocolFactory());
    }

    public static <T extends TBase> T fromJson(T t,String json){
        return fromBytes(t,json.getBytes(), SerializationProtocol.JSON.getTProtocolFactory());
    }
    public static <T extends TBase> T fromJson(Class<T> tClass,String json){
        return fromBytes(tClass, json.getBytes(), SerializationProtocol.JSON.getTProtocolFactory());
    }

    public static  <T extends TBase> byte[] toCompactBytes(T t){
        return toBytes(t, SerializationProtocol.COMPACTBINARAY.getTProtocolFactory());
    }
    public static <T extends TBase> T fromCompactBytes(T t,byte[] bytes){
        return fromBytes(t,bytes, SerializationProtocol.COMPACTBINARAY.getTProtocolFactory());
    }
    public static <T extends TBase> T fromCompactBytes(Class<T> tClass,byte[] bytes){
        return fromBytes(tClass, bytes, SerializationProtocol.COMPACTBINARAY.getTProtocolFactory());
    }




    private static <T extends TBase> byte[] toBytes(T t, TProtocolFactory factory){
        try {
            TSerializer tSerializer = new TSerializer(factory);
            return tSerializer.serialize(t);
        }catch (TException e){
            logger.error("Serializer is error ...",e);
        }
        return null;
    }

    private static <T extends TBase> T fromBytes(T t,byte[] bytes,TProtocolFactory factory){

        try {
            TDeserializer tDeserializer = new TDeserializer(factory);
            tDeserializer.deserialize(t,bytes);
            return t;
        }catch (TException e){
            logger.error("Deserializer is error ...",e);
        }
        return null;
    }

    private static <T extends TBase> T fromBytes(Class<T> tClass,byte[] bytes,TProtocolFactory factory){
        try {
            TBase t = tClass.newInstance();
            return (T)fromBytes(t,bytes,factory);
        }catch (Exception e){
            logger.error("Deserializer is error ...",e);
        }
        return null;
    }


    private enum SerializationProtocol{
        BINARAY(1),
        JSON(2),
        COMPACTBINARAY(3);

        private final int value;

        SerializationProtocol(int value){
            this.value=value;
        }

        TProtocolFactory getTProtocolFactory(){
            switch (value){
                case 1:
                    return new TBinaryProtocol.Factory();
                case 2:
                    return new TJSONProtocol.Factory();
                case 3:
                    return new TCompactProtocol.Factory();
                default:
                    break;
            }
            return null;
        }

    }
}
