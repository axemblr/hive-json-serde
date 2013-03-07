/**
 * JSON SerDe for Hive
 */
package org.apache.hadoop.hive.contrib.serde2;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * <pre>
 * CREATE EXTERNAL TABLE IF NOT EXISTS response_log_example (
 *      request_id string,
 *      keywords string
 * )
 * ROW FORMAT SERDE "org.apache.hadoop.hive.contrib.serde2.JsonSerDe"
 * WITH SERDEPROPERTIES (
 *      "request_id"="$.search_result.requestId",
 *      "keywords"="$['param.keywords']"
 * );
 * </pre>
 */
public class JsonSerDe implements SerDe {
    /**
     * Apache commons logger
     */
    private static final Log LOG = LogFactory.getLog(JsonSerDe.class.getName());

    /**
     * The number of columns in the table this SerDe is being used with
     */
    private int numberOfColumns;

    /**
     * List of column names in the table
     */
    private List<String> columnNames;

    /**
     * An ObjectInspector to be used as meta-data about a deserialized row
     */
    private StructObjectInspector rowObjectInspector;

    /**
     * List of row objects
     */
    private ArrayList<Object> row;

    /**
     * List of column type information
     */
    private List<TypeInfo> columnTypes;

    private Map<String, JsonPath> columnNameJsonPathMap = null;

    /**
     * Initialize this SerDe with the system properties and table properties
     */
    @Override
    public void initialize(Configuration systemProperties, Properties tableProperties)
        throws SerDeException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("JsonSerDe Configuration: " + systemProperties);
            LOG.debug("JsonSerDe Table Properties: " + tableProperties);

        }

        // Get the names of the columns for the table this SerDe is being used with
        String columnNameProperty = tableProperties.getProperty(Constants.LIST_COLUMNS);
        columnNames = Arrays.asList(columnNameProperty.split(","));

        // Convert column types from text to TypeInfo objects
        String columnTypeProperty = tableProperties.getProperty(Constants.LIST_COLUMN_TYPES);
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

        /**
         * Make sure the number of column types and column names are equal.
         */
        assert columnNames.size() == columnTypes.size();
        numberOfColumns = columnNames.size();

        /**
         * Store the mapping between the column name and the JSONPath expression for
         * accessing that column's value within the JSON object.
         */

        // Build a hashmap from column name to JSONPath expression.
        columnNameJsonPathMap = new HashMap<String, JsonPath>();
        String[] propertiesSet = new String[tableProperties.stringPropertyNames().size()];
        propertiesSet = tableProperties.stringPropertyNames().toArray(propertiesSet);

        for (String columnName : columnNames) {
            String currentJsonPath = null;
            for (String property : propertiesSet) {
                if (property.equalsIgnoreCase(columnName)) {
                    currentJsonPath = tableProperties.getProperty(property);
                    break;
                }
            }

            if (currentJsonPath == null) {
                String errorMsg = (String.format("SERDEPROPERTIES must include a property for every column. " +
                    "Missing property for column '%s'.", columnName));
                LOG.error(errorMsg);
                throw new SerDeException(errorMsg);
            }

            JsonPath compiledPath = JsonPath.compile(currentJsonPath);
            if (!compiledPath.isPathDefinite()) {
                throw new SerDeException(String.format("All JSON paths must point to exactly one item.  " +
                    "The following path is ambiguous: %s", currentJsonPath));
            }

            // @todo consider trimming the whitespace from the tokens.
            columnNameJsonPathMap.put(columnName, compiledPath);
        }

        // Create ObjectInspectors from the type information for each column
        List<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(columnNames.size());
        ObjectInspector objectInspector;

        for (int c = 0; c < numberOfColumns; c++) {
            objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(c));
            columnObjectInspectors.add(objectInspector);
        }
        rowObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnObjectInspectors);

        // Create an empty row object to be reused during deserialization
        row = new ArrayList<Object>(numberOfColumns);
        for (int c = 0; c < numberOfColumns; c++) {
            row.add(null);
        }

        LOG.debug("JsonSerDe initialization complete");
    }

    /**
     * Gets the ObjectInspector for a row deserialized by this SerDe
     */
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowObjectInspector;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    /**
     * Deserialize a JSON Object into a row for the table
     */
    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        Text rowText = (Text) blob;

        // Try parsing row into JSON object
        JSONObject jsonObject;
        try {
            jsonObject = (JSONObject) JSONValue.parseWithException(rowText.toString());

        } catch (ParseException e) {
            LOG.error("Failed to parse: " + rowText, e);
            return null;
        }

        // Loop over columns in table and set values
        JsonPath columnPath;
        Object temporaryValue;

        String columnValue;
        Object value;

        for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
            columnPath = columnNameJsonPathMap.get(columnNames.get(columnIndex));
            TypeInfo typeInfo = columnTypes.get(columnIndex);

            try {
                temporaryValue = columnPath.read(jsonObject);
            } catch (InvalidPathException e) {
                temporaryValue = null;
            }

            if (temporaryValue == null) {
                value = null;

            } else {
                columnValue = temporaryValue.toString();

                // Get type-safe JSON values
                if (typeInfo.getTypeName().equalsIgnoreCase(Constants.DOUBLE_TYPE_NAME)) {
                    value = Double.valueOf(columnValue);
                } else if (typeInfo.getTypeName().equalsIgnoreCase(Constants.BIGINT_TYPE_NAME)) {
                    value = Long.valueOf(columnValue);
                } else if (typeInfo.getTypeName().equalsIgnoreCase(Constants.INT_TYPE_NAME)) {
                    value = Integer.valueOf(columnValue);
                } else if (typeInfo.getTypeName().equalsIgnoreCase(Constants.TINYINT_TYPE_NAME)) {
                    value = Byte.valueOf(columnValue);
                } else if (typeInfo.getTypeName().equalsIgnoreCase(Constants.FLOAT_TYPE_NAME)) {
                    value = Float.valueOf(columnValue);
                } else if (typeInfo.getTypeName().equalsIgnoreCase(Constants.BOOLEAN_TYPE_NAME)) {
                    value = Boolean.valueOf(columnValue);
                } else {
                    // Fall back, just use the string
                    value = columnValue;
                }
            }

            row.set(columnIndex, value);
        }

        return row;
    }

    /**
     * Not sure - something to do with serialization of data
     */
    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    /**
     * Serializes a row of data into a JSON object
     */
    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector)
        throws SerDeException {
        throw new UnsupportedOperationException("JSON serialization is not implemented");
    }
}
