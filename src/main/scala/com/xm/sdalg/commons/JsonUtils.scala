package com.xm.sdalg.commons

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonMappingException, JsonNode, ObjectMapper}
import java.io.{IOException, InputStream}
import java.util.{ArrayList, HashMap, List, Map}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging

object JsonUtils extends Logging with Serializable {

    private val objectMapper = new ObjectMapper

    objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
    objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

    /**
     * 转Json字符串.
     *
     * @param `object`
     * @return
     */
    def toJSONString(`object`: Any): String = {
        try return objectMapper.writeValueAsString(`object`)
        catch {
            case e: JsonProcessingException => log.error(e.getMessage, e)
        }
        ""
    }

    /**
     * 将字符串转JsonNode对象.
     *
     * @param value
     * @return
     */
    def parseObject(value: String): JsonNode = {
        try return objectMapper.readTree(value)
        catch {
            case e: IOException => log.error(e.getMessage, e)
        }
        emptyJsonNode
    }

    /**
     * 从JsonNode中获取指定FieldName的字符串数值.
     *
     * @param jsonNode
     * @param fieldName
     * @param default
     * @return
     */
    def getStringValue(jsonNode: JsonNode, fieldName: String, default: String = StringUtils.EMPTY): String = {
        val node = jsonNode.get(fieldName)
        if (null == node) default
        else node.asText(default)
    }

    /**
     * 创建空JsonNode.
     *
     * @return
     */
    def emptyJsonNode: ObjectNode = objectMapper.createObjectNode

    /**
     * 创建空ArrayNode.
     *
     * @return
     */
    def emptyArrayNode: ArrayNode = objectMapper.createArrayNode

    /**
     * 解析字符串为对象数组.
     *
     * @param value
     * @param clazz
     * @tparam T
     * @return
     */
    def parseArray[T](value: String, clazz: Class[T]): List[T] = {
        try return objectMapper.readValue(value,
            objectMapper.getTypeFactory.constructCollectionType(classOf[List[_]], clazz))
        catch {
            case e: IOException => log.error(e.getMessage, e)
        }
        new ArrayList[T]
    }

    /**
     * 解析字节数组为对象数组.
     *
     * @param value
     * @param clazz
     * @tparam T
     * @return
     */
    def parseArray[T](value: Array[Byte], clazz: Class[T]): List[T] = {
        try return objectMapper.readValue(value,
            objectMapper.getTypeFactory.constructCollectionType(classOf[List[_]], clazz))
        catch {
            case e: IOException => log.error(e.getMessage, e)
        }
        new ArrayList[T]
    }

    /**
     * 解析字符串为Map对象数组.
     *
     * @param value
     * @tparam K
     * @tparam V
     * @return
     */
    def parseMap[K, V](value: String): Map[K, V] = {
        val typeRef: TypeReference[HashMap[K, V]] = new TypeReference[HashMap[K, V]]() {}
        try return objectMapper.readValue(value, typeRef)
        catch {
            case e: IOException => log.error(e.getMessage, e)
        }
        new HashMap[K, V]
    }

    /**
     * 解析InputStream为JsonNode.
     *
     * @param ins
     * @return
     */
    def parseObject(ins: InputStream): JsonNode = {
        try return objectMapper.readTree(ins)
        catch {
            case e: IOException => log.error(e.getMessage, e)
        }
        emptyJsonNode
    }

    /**
     * 解析InputStream为对象.
     *
     * @param ins
     * @param clazz
     * @tparam T
     * @throws
     * @return
     */
    @throws[IOException]
    def parseObject[T](ins: InputStream, clazz: Class[T]): T = objectMapper.readValue(ins, clazz)

}
