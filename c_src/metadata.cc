//
//  metadata.cpp
//  erlcass
//
//  Created by silviu on 5/13/15.
//
//

#include "metadata.h"
#include "erlcass.h"
#include "utils.h"
#include <string>

CassValueType GetValueTypeFromAtom(ERL_NIF_TERM value)
{
    if(enif_is_identical(value, ATOMS.atomText))
        return CASS_VALUE_TYPE_TEXT;
    
    if(enif_is_identical(value, ATOMS.atomInt))
        return CASS_VALUE_TYPE_INT;
    
    if(enif_is_identical(value, ATOMS.atomBigInt))
        return CASS_VALUE_TYPE_BIGINT;
    
    if(enif_is_identical(value, ATOMS.atomBlob))
        return CASS_VALUE_TYPE_BLOB;
    
    if(enif_is_identical(value, ATOMS.atomBool))
        return CASS_VALUE_TYPE_BOOLEAN;
    
    if(enif_is_identical(value, ATOMS.atomFloat))
        return CASS_VALUE_TYPE_FLOAT;
    
    if(enif_is_identical(value, ATOMS.atomDouble))
        return CASS_VALUE_TYPE_DOUBLE;
    
    if(enif_is_identical(value, ATOMS.atomInet))
        return CASS_VALUE_TYPE_INET;
    
    if(enif_is_identical(value, ATOMS.atomUuid))
        return CASS_VALUE_TYPE_UUID;

    if(enif_is_identical(value, ATOMS.atomDecimal))
        return CASS_VALUE_TYPE_DECIMAL;
 
    return CASS_VALUE_TYPE_UNKNOWN;
}

SchemaColumn atom_to_cass_value_type(ErlNifEnv* env, ERL_NIF_TERM value)
{
    if(enif_is_tuple(env, value))
    {
        const ERL_NIF_TERM *items;
        int arity;
        
        if(enif_get_tuple(env, value, &arity, &items))
        {
            if(arity == 2)
            {
                if(enif_is_identical(ATOMS.atomList, items[0]))
                    return SchemaColumn(CASS_VALUE_TYPE_LIST, GetValueTypeFromAtom(items[1]));
                else if(enif_is_identical(ATOMS.atomSet, items[0]))
                    return SchemaColumn(CASS_VALUE_TYPE_SET, GetValueTypeFromAtom(items[1]));
            }
            else if (arity == 3 && enif_is_identical(ATOMS.atomMap, items[0]))
            {
                return SchemaColumn(CASS_VALUE_TYPE_MAP, GetValueTypeFromAtom(items[1]), GetValueTypeFromAtom(items[2]));
            }
        }
        
        return SchemaColumn(GetValueTypeFromAtom(CASS_VALUE_TYPE_UNKNOWN));
    }
    
    //non collection
    
    return SchemaColumn(GetValueTypeFromAtom(value));
}

bool parse_statement_metadata(ErlNifEnv* env, ERL_NIF_TERM list, ColumnsMap* metadata)
{
    metadata->clear();
    
    ERL_NIF_TERM head;
    const ERL_NIF_TERM *items;
    int arity;
    
    std::string column_name;
    
    while(enif_get_list_cell(env, list, &head, &list))
    {
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return false;
        
        if(!get_string(env, items[0], column_name))
            return false;
        
        SchemaColumn type = atom_to_cass_value_type(env, items[1]);
        
        if(type.valueType == CASS_VALUE_TYPE_UNKNOWN)
            return false;
        
        (*metadata)[column_name] = type;
    }
    
    return true;
}
