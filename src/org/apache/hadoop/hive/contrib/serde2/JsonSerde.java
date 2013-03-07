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
import org.json.JSONException;
import org.json.JSONObject;

/**
 * JSON SerDe for Hive
 * <p/>
 * This SerDe can be used to read data in JSON format. For example, if your JSON
 * files had the following contents:
 * <p/>
 * <pre>
 * {"field1":"data1","field2":100,"field3":"more data1"}
 * {"field1":"data2","field2":200,"field3":"more data2"}
 * {"field1":"data3","field2":300,"field3":"more data3"}
 * {"field1":"data4","field2":400,"field3":"more data4"}
 * </pre>
 * <p/>
 * The following steps can be used to read this data:
 * <ol>
 * <li>Build this project using <code>ant build</code></li>
 * <li>Copy <code>hive-json-serde.jar</code> to the Hive server</li>
 * <li>Inside the Hive client, run
 * <p/>
 * <pre>
 * ADD JAR /home/hadoop/hive-json-serde.jar;
 * </pre>
 * <p/>
 * </li>
 * <li>Create a table that uses files where each line is JSON object
 * <p/>
 * <pre>
 * CREATE EXTERNAL TABLE IF NOT EXISTS my_table (
 *    field1 string, field2 int, field3 string
 * )
 * ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde'
 * LOCATION '/my_data/my_table/';
 * </pre>
 * <p/>
 * </li>
 * <li>Copy your JSON files to <code>/my_data/my_table/</code>. You can now
 * select data using normal SELECT statements
 * <p/>
 * <pre>
 * SELECT * FROM my_table LIMIT 10;
 * </pre>
 * <p/>
 * <p/>
 * </li>
 * </ol>
 * <p/>
 * The table does not have to have the same columns as the JSON files, and
 * vice-versa. If the table has a column that does not exist in the JSON object,
 * it will have a NULL value. If the JSON file contains fields that are not
 * columns in the table, they will be ignored and not visible to the table.
 *
 * @author Peter Sankauskas
 * @see <a href="http://code.google.com/p/hive-json-serde/">hive-json-serde on
 *      Google Code</a>
 */
public class JsonSerde implements SerDe {
    /**
     * Apache commons logger
     */
    private static final Log LOG = LogFactory.getLog(JsonSerde.class.getName());

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
    private StructObjectInspector rowObjetInspector;

    /**
     * List of row objects
     */
    private ArrayList<Object> row;

    /**
     * List of column type information
     */
    private List<TypeInfo> columnTypes;

    private Properties tableProperties;
    private Map<String, JsonPath> colNameJsonPathMap = null;

    /**
     * Initialize this SerDe with the system properties and table properties
     */
    @Override
    public void initialize(Configuration sysProps, Properties tableProperties)
        throws SerDeException {
        LOG.debug("Initializing JsonSerde");

        this.tableProperties = tableProperties;

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
        colNameJsonPathMap = new HashMap<String, JsonPath>();
        String[] propertiesSet = new String[tableProperties.stringPropertyNames().size()];
        propertiesSet = tableProperties.stringPropertyNames().toArray(propertiesSet);
        String currentJsonPath;

        int z = 0;
        for (String colName : columnNames) {
            LOG.debug("Iteration #" + z);
            currentJsonPath = null;
            for (String aPropertiesSet : propertiesSet) {
                LOG.debug("current property=" + aPropertiesSet);
                if (aPropertiesSet.equalsIgnoreCase(colName)) {
                    currentJsonPath = tableProperties.getProperty(aPropertiesSet);
                    break;
                }
            }

            if (currentJsonPath == null) {
                String errorMsg = (String.format("SERDEPROPERTIES must include a property for every column. " +
                    "Missing property for column '%s'.", colName));
                LOG.error(errorMsg);
                throw new SerDeException(errorMsg);
            }

            LOG.info("Checking JSON path=" + currentJsonPath);

            JsonPath compiledPath = JsonPath.compile(currentJsonPath);
            if (!compiledPath.isPathDefinite()) {
                throw new SerDeException(String.format("All JSON paths must point to exactly one item.  " +
                    "The following path is ambiguous: %s", currentJsonPath));
            }

            LOG.debug("saving json path=" + currentJsonPath);
            // @todo consider trimming the whitespace from the tokens.
            colNameJsonPathMap.put(colName, compiledPath);
            z++;
        }

        // Create ObjectInspectors from the type information for each column
        List<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(columnNames.size());
        ObjectInspector objectInspector;

        for (int c = 0; c < numberOfColumns; c++) {
            objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(c));
            columnObjectInspectors.add(objectInspector);
        }
        rowObjetInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnObjectInspectors);

        // Create an empty row object to be reused during deserialization
        row = new ArrayList<Object>(numberOfColumns);
        for (int c = 0; c < numberOfColumns; c++) {
            row.add(null);
        }

        LOG.debug("JsonSerde initialization complete");
    }

    /**
     * Gets the ObjectInspector for a row deserialized by this SerDe
     */
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowObjetInspector;
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
            jsonObject = new JSONObject(rowText.toString()) {
                /**
                 * In Hive column names are case insensitive, so lower-case all
                 * field names
                 *
                 * @see org.json.JSONObject#put(java.lang.String, java.lang.Object)
                 */
                @Override
                public JSONObject put(String key, Object value)
                    throws JSONException {
                    return super.put(key.toLowerCase(), value);
                }
            };

        } catch (JSONException e) {
            // If row is not a JSON object, make the whole row NULL
            LOG.error("Row is not a valid JSON Object - JSONException: " + e.getMessage());
            return null;
        }

        // Loop over columns in table and set values
        JsonPath columnPath;
        String jsonStr = jsonObject.toString();

        Object temporaryValue;
        String columnValue;
        Object value;

        for (int c = 0; c < numberOfColumns; c++) {
            columnPath = colNameJsonPathMap.get(columnNames.get(c));
            TypeInfo typeInfo = columnTypes.get(c);

            try {
                temporaryValue = columnPath.read(jsonStr);
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

            row.set(c, value);
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
