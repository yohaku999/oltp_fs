#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>

class Column {
 public:
  enum class Type { Integer, Double, Varchar };

  using IntegerType = std::int32_t;
  using DoubleType = double;
  using VarcharType = std::string;

  Column(std::string name, Type type)
      : name_(std::move(name)),
        type_(type),
        is_variable_length_(inferVariableLength(type)) {}

  std::size_t size() const {
    switch (type_) {
      case Type::Integer:
        return sizeof(IntegerType);
      case Type::Double:
        return sizeof(DoubleType);
      case Type::Varchar:
        return sizeof(VarcharType);
    }
    throw std::runtime_error("Unknown column type");
  }

  const std::string& getName() const { return name_; }

  Type getType() const { return type_; }

  static std::string typeToString(Type type) {
    switch (type) {
      case Type::Integer:
        return "Integer";
      case Type::Double:
        return "Double";
      case Type::Varchar:
        return "Varchar";
    }
    throw std::runtime_error("Unknown column type");
  }

  static Type typeFromString(const std::string& type_name) {
    if (type_name == "Integer") {
      return Type::Integer;
    }
    if (type_name == "Double") {
      return Type::Double;
    }
    if (type_name == "Varchar") {
      return Type::Varchar;
    }
    throw std::runtime_error("Unknown column type: " + type_name);
  }

  bool isFixedLength() const { return !is_variable_length_; }

 private:
  static bool inferVariableLength(Type type) {
    switch (type) {
      case Type::Integer:
        return false;
      case Type::Double:
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
