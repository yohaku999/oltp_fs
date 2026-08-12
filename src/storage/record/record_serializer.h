#pragma once

#include "record_cell.h"
#include "schema/schema.h"
#include "tuple/typed_row.h"

class RecordSerializer {
 public:
  static RecordCell serialize(const Schema& schema, const TypedRow& row);
};
