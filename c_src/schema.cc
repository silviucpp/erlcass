//
//  schema.cpp
//  erlcass
//
//  Created by silviu on 6/5/15.
//
//

#include "schema.h"
#include <cstring>

const char kValidatorAscii[] = "org.apache.cassandra.db.marshal.AsciiType";
const char kValidatorUUID[] = "org.apache.cassandra.db.marshal.UUIDType";
const char kValidatorUTF8[] = "org.apache.cassandra.db.marshal.UTF8Type";
const char kValidatorInteger[] = "org.apache.cassandra.db.marshal.IntegerType";
const char kValidatorTimeUUID[] = "org.apache.cassandra.db.marshal.TimeUUIDType";
const char kValidatorInetAddress[] = "org.apache.cassandra.db.marshal.InetAddressType";
const char kValidatorLong[] = "org.apache.cassandra.db.marshal.LongType";
const char kValidatorBytes[] = "org.apache.cassandra.db.marshal.BytesType";
const char kValidatorBoolean[] = "org.apache.cassandra.db.marshal.BooleanType";
const char kValidatorDecimal[] = "org.apache.cassandra.db.marshal.DecimalType";
const char kValidatorDouble[] = "org.apache.cassandra.db.marshal.DoubleType";
const char kValidatorFloat[] = "org.apache.cassandra.db.marshal.FloatType";
const char kValidatorInt32[] = "org.apache.cassandra.db.marshal.Int32Type";
const char kValidatorTimestamp[] = "org.apache.cassandra.db.marshal.TimestampType";
const char kValidatorDate[] = "org.apache.cassandra.db.marshal.DateType";
const char kValidatorSet[] = "org.apache.cassandra.db.marshal.SetType";
const char kValidatorList[] = "org.apache.cassandra.db.marshal.ListType";
const char kValidatorMap[] = "org.apache.cassandra.db.marshal.MapType";
const char kValidatorCounter[] = "org.apache.cassandra.db.marshal.CounterColumnType";
const char kValidatorReversedType[] = "org.apache.cassandra.db.marshal.ReversedType";

#define VALIDATOR_MAP_INSERT(Key, Value) map[Key] = Value

std::map<std::string, CassValueType> create_validators_map()
{
	std::map<std::string, CassValueType> map;

	VALIDATOR_MAP_INSERT(kValidatorAscii, CASS_VALUE_TYPE_ASCII);
	VALIDATOR_MAP_INSERT(kValidatorLong, CASS_VALUE_TYPE_BIGINT);
	VALIDATOR_MAP_INSERT(kValidatorBytes, CASS_VALUE_TYPE_BLOB);
	VALIDATOR_MAP_INSERT(kValidatorBoolean, CASS_VALUE_TYPE_BOOLEAN);
	VALIDATOR_MAP_INSERT(kValidatorCounter, CASS_VALUE_TYPE_COUNTER);
	VALIDATOR_MAP_INSERT(kValidatorDecimal, CASS_VALUE_TYPE_DECIMAL);
	VALIDATOR_MAP_INSERT(kValidatorDouble, CASS_VALUE_TYPE_DOUBLE);
	VALIDATOR_MAP_INSERT(kValidatorFloat, CASS_VALUE_TYPE_FLOAT);
	VALIDATOR_MAP_INSERT(kValidatorInt32, CASS_VALUE_TYPE_INT);
	VALIDATOR_MAP_INSERT(kValidatorUTF8, CASS_VALUE_TYPE_TEXT);
	VALIDATOR_MAP_INSERT(kValidatorTimestamp, CASS_VALUE_TYPE_TIMESTAMP);
	VALIDATOR_MAP_INSERT(kValidatorDate, CASS_VALUE_TYPE_TIMESTAMP);
	VALIDATOR_MAP_INSERT(kValidatorUUID, CASS_VALUE_TYPE_UUID);
	VALIDATOR_MAP_INSERT(kValidatorInteger, CASS_VALUE_TYPE_VARINT);
	VALIDATOR_MAP_INSERT(kValidatorTimeUUID, CASS_VALUE_TYPE_TIMEUUID);
	VALIDATOR_MAP_INSERT(kValidatorInetAddress, CASS_VALUE_TYPE_INET);

	return map;
}

const std::map<std::string, CassValueType> kValidatorsMap = create_validators_map();

class SchemaScope
{
public:
    
    SchemaScope(const CassSchema* schema) : schema_(schema) {}
    ~SchemaScope() {cass_schema_free(schema_);}
    
    const CassSchema* get() const {return schema_;}
    
private:
    
    const CassSchema* schema_;
};

std::string get_subtype(const std::string& validator, const char begin, const char end)
{
    size_t start_index = validator.find(begin);
    size_t end_index = validator.find(end);
    
    if(start_index == std::string::npos || end_index == std::string::npos)
        return "";
    
    start_index = start_index + 1;
    
    return validator.substr(start_index, end_index - start_index);
}

SchemaColumn validator_to_cass_value_type(const std::string & validator)
{
    std::map<std::string, CassValueType>::const_iterator it = kValidatorsMap.find(validator);
    
    if(it == kValidatorsMap.end())
    {
        //test for collections or reversed types
        
        static size_t set_length = strlen(kValidatorSet);
        static size_t list_length = strlen(kValidatorList);
        static size_t map_length = strlen(kValidatorMap);
        static size_t reversed_type_length = strlen(kValidatorReversedType);
        
        if (validator.compare(0, reversed_type_length, kValidatorReversedType) == 0)
        {
            CassValueType subtype = validator_to_cass_value_type(get_subtype(validator, '(', ')')).type;
            
            if(subtype != CASS_VALUE_TYPE_UNKNOWN)
                return SchemaColumn(subtype);
            
            return SchemaColumn();
        }
        
        if (validator.compare(0, list_length, kValidatorList) == 0)
        {
            CassValueType subtype = validator_to_cass_value_type(get_subtype(validator, '(', ')')).type;
            
            if(subtype != CASS_VALUE_TYPE_UNKNOWN)
                return SchemaColumn(CASS_VALUE_TYPE_LIST, subtype);
            
            return SchemaColumn();
        }
        
        if (validator.compare(0, set_length, kValidatorSet) == 0)
        {
            CassValueType subtype = validator_to_cass_value_type(get_subtype(validator, '(', ')')).type;
            
            if(subtype != CASS_VALUE_TYPE_UNKNOWN)
                return SchemaColumn(CASS_VALUE_TYPE_SET, subtype);
            
            return SchemaColumn();
        }
        
        if (validator.compare(0, map_length, kValidatorMap) == 0)
        {
            CassValueType keySubtype = validator_to_cass_value_type(get_subtype(validator, '(', ',')).type;
            CassValueType valueSubtype = validator_to_cass_value_type(get_subtype(validator, ',', ')')).type;
            
            if(keySubtype != CASS_VALUE_TYPE_UNKNOWN && valueSubtype != CASS_VALUE_TYPE_UNKNOWN)
                return SchemaColumn(CASS_VALUE_TYPE_MAP, keySubtype, valueSubtype);
            
            return SchemaColumn();
        }
                
        return SchemaColumn();
    }
    
    return SchemaColumn(it->second);
}


bool get_schema_field_value(const CassSchemaMeta* schema, const char * name, std::string* value)
{
    const CassSchemaMetaField* field = cass_schema_meta_get_field(schema, name);
    
    const char* buffer;
    size_t buffer_length;
    
    if(cass_value_get_string(cass_schema_meta_field_value(field), &buffer, &buffer_length) != CASS_OK)
        return false;
    
    *value = std::string(buffer, buffer_length);
    return true;
}

bool get_table_schema(CassSession* session, const std::string& keyspace, const std::string& table, ColumnsMap* schema_map)
{
    SchemaScope ss(cass_session_get_schema(session));
    
    if(!ss.get())
        return false;
    
    const CassSchemaMeta* keyspace_meta = cass_schema_get_keyspace(ss.get(), keyspace.c_str());
    
    if(!keyspace_meta)
        return false;
    
    const CassSchemaMeta* table_meta = cass_schema_meta_get_entry(keyspace_meta, table.c_str());
    
    if(!table_meta)
        return false;
    
    CassIterator* entries = cass_iterator_from_schema_meta(table_meta);
    
    bool success = true;
    
    std::string column_name;
    std::string validator;
    
    while (cass_iterator_next(entries))
    {
        const CassSchemaMeta* schema_meta = cass_iterator_get_schema_meta(entries);
        
        if(!get_schema_field_value(schema_meta, "column_name", &column_name) ||
           !get_schema_field_value(schema_meta, "validator", &validator))
        {
            success = false;
            break;
        }
        
        SchemaColumn type = validator_to_cass_value_type(validator);
        
        if(type.type == CASS_VALUE_TYPE_UNKNOWN)
        {
            success = false;
            break;
        }
        
        (*schema_map)[column_name] = type;
    }
    
    cass_iterator_free(entries);
    
    return success;
}
