package jp.growthfield.hive.udaf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Aggregate concatnation function that is similar to Oracle's wm_concat function.
 *
 * @author growthfield
 *
 */
public class GenericUDAFListAgg implements GenericUDAFResolver2 {

    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        TypeInfo[] parameters = info.getParameters();
        if (parameters.length < 1 || parameters.length > 2) {
            throw new UDFArgumentException("One or two arguments are expected.");
        }
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted.");
        }			
        if (parameters.length == 2) {
            if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(1, "Only numeric type arguments are accepted");							
            }
            PrimitiveCategory pc = ((PrimitiveTypeInfo)parameters[1]).getPrimitiveCategory();
            if (pc != PrimitiveObjectInspector.PrimitiveCategory.LONG && 
                    pc != PrimitiveObjectInspector.PrimitiveCategory.INT) {
                throw new UDFArgumentTypeException(1, "Only long or int type arguments are accepted");	
                    }
        }
        return new GenericListAggEval();
    }

    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        return new GenericListAggEval();
    }

    public static class GenericListAggEval extends GenericUDAFEvaluator {

        private ObjectInspector[] inputInspectors;
        private ObjectInspector parameter1InputInspector;
        private ObjectInspector parameter2InputInspector;
        private StandardListObjectInspector terminatePartialOutputInspector;
        private ObjectInspector terminateOutputInspector;
        private Converter aggValueConverter;
        private Converter aggOrderConverter;

        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {                                                                                                                                           
            super.init(m, parameters);
            this.inputInspectors = parameters;
            // Determine input object inspector
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                this.parameter1InputInspector = parameters[0];
                if (parameters.length == 2) {
                    this.parameter2InputInspector = parameters[1];
                }
            } else if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
                StandardListObjectInspector loi = (StandardListObjectInspector)parameters[0];
                StructObjectInspector soi = (StructObjectInspector)loi.getListElementObjectInspector();
                this.parameter1InputInspector = soi.getStructFieldRef("value").getFieldObjectInspector();
                this.parameter2InputInspector = soi.getStructFieldRef("order").getFieldObjectInspector();
            }
            this.terminatePartialOutputInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorFactory.getReflectionObjectInspector(ListAggElement.class, ObjectInspectorOptions.JAVA)
                    );
            // Prepare converters for iterate proc
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                this.aggValueConverter = 
                    ObjectInspectorConverters.getConverter(this.parameter1InputInspector, PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                this.aggOrderConverter = 
                    ObjectInspectorConverters.getConverter(this.parameter2InputInspector, PrimitiveObjectInspectorFactory.javaIntObjectInspector);
            }   
            // Determine output object inspector
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                // for terminatePartial proc
                return this.terminatePartialOutputInspector;
            } else if (m == Mode.COMPLETE || m == Mode.FINAL) {
                // for terminate proc
                this.terminateOutputInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                return this.terminateOutputInspector;
            } else {
                throw new IllegalArgumentException("Invalid mode. " + m.toString()); 
            }
        }

        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            ListAggBuff buff = new ListAggBuff();
            reset(buff);
            return buff;
        }

        public void reset(AggregationBuffer agg) throws HiveException {
            ListAggBuff buff = (ListAggBuff)agg;
            buff.list = new ArrayList<ListAggElement>();
        }

        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            ListAggBuff buff = (ListAggBuff)agg;
            ListAggElement elem = new ListAggElement();
            elem.value = (String)this.aggValueConverter.convert(parameters[0]);
            if (parameters.length == 2) {
                elem.order = (Integer)this.aggOrderConverter.convert(parameters[1]);
            }
            buff.list.add(elem);
        }

        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            ListAggBuff buff = (ListAggBuff)agg;
            return new ArrayList<ListAggElement>(buff.list);
        }

        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            ListAggBuff buff = (ListAggBuff)agg;
            Converter conv = ObjectInspectorConverters.getConverter(this.inputInspectors[0], this.terminatePartialOutputInspector);
            Object array = conv.convert(partial);
            for (Object o: this.terminatePartialOutputInspector.getList(array)) {
                ListAggElement elem = (ListAggElement)o;
                buff.list.add(elem);
            }
        }

        public Object terminate(AggregationBuffer agg) throws HiveException {
            ListAggBuff buff = (ListAggBuff)agg;
            Collections.sort(buff.list, new Comparator<ListAggElement>() {
                public int compare(ListAggElement e1, ListAggElement e2) {
                    return e1.order - e2.order;
                }
            });
            StringBuilder sb = new StringBuilder();
            for (ListAggElement e: buff.list) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(e.value);
            }
            return sb.toString();
        }
    }

    static class ListAggBuff implements AggregationBuffer {
        List<ListAggElement> list = new ArrayList<ListAggElement>();
    }

    static class ListAggElement {
        String value;
        int order;
    }

}
