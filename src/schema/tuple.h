#pragma once
#include "column.h"
#include "schema.h"
#include "../storage/record_cell.h"

class Tuple {

public:
    static std::string getValuesAsString(const char* cell_start, const Schema& schema, const std::string& column_name) {
        RecordCellView view(cell_start);

        // get column from schema.
        const Column* target_column = nullptr;
        for(const auto& column : schema.columns_) {
            if(column.getName() == column_name) {
                target_column = &column;
                break;
            }
        }

        // get value
        // for fixed length, we can directly read the value based on the offset of the value in the record cell.
        if (target_column->isFixedLength()) {
            const char* target_column_begin = view.getFixedPayloadBegin();
            for (std::size_t i = 0; i < schema.columns_.size(); ++i) {
                const auto& column = schema.columns_[i];
                if (&column == target_column) {
                    break;
                }
                if (column.isFixedLength()) {
                    target_column_begin += schema.columns_[i].size();
                }
            }
            // convert to real value.
            std::vector<char> buffer(target_column->size());
            std::memcpy(buffer.data(), target_column_begin, target_column->size());
            return std::string(buffer.data(), target_column->size());
        }else{
            // 
            int target_column_index = schema.getVariableColumnIndex(column_name);
            const char* target_column_begin;
            uint16_t target_column_size;
            std::tie(target_column_begin, target_column_size) = view.getXthVariableColumnbegin(target_column_index, schema.getVariableColumnCount());
            // convert to real value.
            std::vector<char> buffer(target_column_size);
            std::memcpy(buffer.data(), target_column_begin, target_column_size);
            return std::string(buffer.data(), target_column_size);

        }
    }
    
};