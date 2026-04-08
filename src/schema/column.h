#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>

class Column {
 public:
  enum class Type { Integer, Varchar };

  using IntegerType = std::int32_t;
  using VarcharType = std::string;

  Column(std::string name, Type type)
      : name_(std::move(name)),
        type_(type),
        is_variable_length_(inferVariableLength(type)) {}

  std::size_t size() const {
    switch (type_) {
      case Type::Integer:
        return sizeof(IntegerType);
      case Type::Varchar:
        return sizeof(VarcharType);
    }
    throw std::runtime_error("Unknown column type");
  }

  const std::string& getName() const { return name_; }

  Type getType() const { return type_; }

  bool isFixedLength() const { return !is_variable_length_; }

 private:
  static bool inferVariableLength(Type type) {
    switch (type) {
      case Type::Integer:
        return false;
      case Type::Varchar:
        return true;
    }
    throw std::runtime_error("Unknown column type");
  }

  std::string name_;
  Type type_;
  bool is_variable_length_;
};
