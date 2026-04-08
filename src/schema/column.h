#pragma once
#include <string>

class Column {
 public:
  enum class Type { Integer, Varchar };

  using IntegerType = std::int32_t;
  using VarcharType = std::string;

  Column(std::string name, Type type) : name_(std::move(name)), type_(type) {}

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

  bool isFixedLength() const { return type_ == Type::Integer; }

 private:
  std::string name_;
  Type type_;
};
