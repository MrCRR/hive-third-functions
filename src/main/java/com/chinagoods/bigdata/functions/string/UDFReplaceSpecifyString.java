package com.chinagoods.bigdata.functions.string;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author CeroChen
 * date: 2021-11-09
 * time: 15:10
 */
@Description(name = "replace_specify_str",
             value = "_FUNC_(string, array, string) - replace specify string in the input array(auto include null value) with specify value ",
//        value = "_FUNC_(string, array, string)",
             extended = "Example:\n+   > SELECT _FUNC_(string, array, string) FROM src;")
public class UDFReplaceSpecifyString extends GenericUDF {
    public static final Logger logger = LoggerFactory.getLogger(UDFReplaceSpecifyString.class);
    private transient Text result = new Text();

    private static final int SRC_STRING_IDX = 0;
    private static final int ARRAY_IDX = 1;
    private static final int SPE_STRING_IDX = 2;
    private static final int ARG_COUNT = 3; // Number of arguments to this UDF
    private transient ObjectInspector srcOI;
    private transient ObjectInspector speOI;
    private transient ListObjectInspector arrayOI;
    private transient ObjectInspector arrayElementOI;

    public UDFReplaceSpecifyString() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Check if three arguments were passed
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentLengthException(
                    "The function replace_specify_str(string, array, string) takes exactly " + ARG_COUNT + " arguments.");
        }

        // Check if ARRAY_IDX argument is of category LIST
        if (!(arguments[ARRAY_IDX].getCategory().equals(ObjectInspector.Category.LIST))) {
            throw new UDFArgumentTypeException(ARRAY_IDX,
                    "\"" + org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "\" "
                            + "expected at function replace_specify_str, but "
                            + "\"" + arguments[ARRAY_IDX].getTypeName() + "\" "
                            + "is found");
        }

        srcOI = arguments[SRC_STRING_IDX];
        arrayOI = (ListObjectInspector) arguments[ARRAY_IDX];
        arrayElementOI = arrayOI.getListElementObjectInspector();
        speOI = arguments[SPE_STRING_IDX];

//        ObjectInspector returnOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
//        return ObjectInspectorFactory.getStandardListObjectInspector(returnOI);
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object src = arguments[SRC_STRING_IDX].get();
        Object array = arguments[ARRAY_IDX].get();
        Object spe = arguments[SPE_STRING_IDX].get();

        int arrayLength = arrayOI.getListLength(array);

        // 源数据为null 返回指定值
        if (src == null){
            result.set(spe.toString());
        }
        else{
            result.set(src.toString());
        // 源数据与替换数组中任意匹配 返回指定值
            for (int i=0;i<arrayLength;i++){
                Object listElement = arrayOI.getListElement(array, i);
                if (ObjectInspectorUtils.compare(src, srcOI, listElement, arrayElementOI) == 0){
                    result.set(spe.toString());
                    break;
                }
            }
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return String.format("replace_specify_str(%s, %s, %s)",
                strings[SRC_STRING_IDX], strings[ARRAY_IDX], strings[SPE_STRING_IDX]);
    }

    public static void main(String[] args) throws HiveException {
        DeferredObject[] testArr = new DeferredObject[3];
        testArr[0] = new GenericUDF.DeferredJavaObject("0");
        testArr[1] = new GenericUDF.DeferredJavaObject(new String[]{"", "0", "-"});
        testArr[2] = new GenericUDF.DeferredJavaObject("0000");
        UDFReplaceSpecifyString udf = new UDFReplaceSpecifyString();
        System.out.println(udf.evaluate(testArr));
    }

}
