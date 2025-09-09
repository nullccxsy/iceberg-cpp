/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "iceberg/schema.h"

#include <format>
#include <functional>

#include "iceberg/schema_internal.h"
#include "iceberg/type.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"
#include "iceberg/util/visit_type.h"

namespace iceberg {

class IdToFieldVisitor {
 public:
  explicit IdToFieldVisitor(
      std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>>&
          id_to_field);
  Status Visit(const PrimitiveType& type);
  Status Visit(const NestedType& type);

 private:
  std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>>& id_to_field_;
};

class NameToIdVisitor {
 public:
  explicit NameToIdVisitor(
      std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>& name_to_id,
      bool case_sensitive = true,
      std::function<std::string(std::string_view)> quoting_func = {});
  Status Visit(const ListType& type, const std::string& path,
               const std::string& short_path);
  Status Visit(const MapType& type, const std::string& path,
               const std::string& short_path);
  Status Visit(const StructType& type, const std::string& path,
               const std::string& short_path);
  Status Visit(const PrimitiveType& type, const std::string& path,
               const std::string& short_path);
  void Finish();

 private:
  std::string BuildPath(std::string_view prefix, std::string_view field_name,
                        bool case_sensitive);

 private:
  bool case_sensitive_;
  std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>& name_to_id_;
  std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>> short_name_to_id_;
  std::function<std::string(std::string_view)> quoting_func_;
};

Schema::Schema(std::vector<SchemaField> fields, std::optional<int32_t> schema_id)
    : StructType(std::move(fields)), schema_id_(schema_id) {}

std::optional<int32_t> Schema::schema_id() const { return schema_id_; }

std::string Schema::ToString() const {
  std::string repr = "schema<";
  for (const auto& field : fields_) {
    std::format_to(std::back_inserter(repr), "  {}\n", field);
  }
  repr += ">";
  return repr;
}

bool Schema::Equals(const Schema& other) const {
  return schema_id_ == other.schema_id_ && fields_ == other.fields_;
}

Result<std::optional<std::reference_wrapper<const SchemaField>>> Schema::FindFieldByName(
    std::string_view name, bool case_sensitive) const {
  if (case_sensitive) {
    ICEBERG_RETURN_UNEXPECTED(
        LazyInitWithCallOnce(name_to_id_flag_, [this]() { return InitNameToIdMap(); }));
    auto it = name_to_id_.find(name);
    if (it == name_to_id_.end()) return std::nullopt;
    return FindFieldById(it->second);
  }
  ICEBERG_RETURN_UNEXPECTED(LazyInitWithCallOnce(
      lowercase_name_to_id_flag_, [this]() { return InitLowerCaseNameToIdMap(); }));
  auto it = lowercase_name_to_id_.find(StringUtils::ToLower(name));
  if (it == lowercase_name_to_id_.end()) return std::nullopt;
  return FindFieldById(it->second);
}

Status Schema::InitIdToFieldMap() const {
  if (!id_to_field_.empty()) {
    return {};
  }
  IdToFieldVisitor visitor(id_to_field_);
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*this, &visitor));
  return {};
}

Status Schema::InitNameToIdMap() const {
  if (!name_to_id_.empty()) {
    return {};
  }
  NameToIdVisitor visitor(name_to_id_, /*case_sensitive=*/true);
  ICEBERG_RETURN_UNEXPECTED(
      VisitTypeInline(*this, &visitor, /*path=*/"", /*short_path=*/""));
  visitor.Finish();
  return {};
}

Status Schema::InitLowerCaseNameToIdMap() const {
  if (!lowercase_name_to_id_.empty()) {
    return {};
  }
  NameToIdVisitor visitor(lowercase_name_to_id_, /*case_sensitive=*/false);
  ICEBERG_RETURN_UNEXPECTED(
      VisitTypeInline(*this, &visitor, /*path=*/"", /*short_path=*/""));
  visitor.Finish();
  return {};
}

Result<std::optional<std::reference_wrapper<const SchemaField>>> Schema::FindFieldById(
    int32_t field_id) const {
  ICEBERG_RETURN_UNEXPECTED(
      LazyInitWithCallOnce(id_to_field_flag_, [this]() { return InitIdToFieldMap(); }));
  auto it = id_to_field_.find(field_id);
  if (it == id_to_field_.end()) {
    return std::nullopt;
  }
  return it->second;
}

IdToFieldVisitor::IdToFieldVisitor(
    std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>>& id_to_field)
    : id_to_field_(id_to_field) {}

Status IdToFieldVisitor::Visit(const PrimitiveType& type) { return {}; }

Status IdToFieldVisitor::Visit(const NestedType& type) {
  const auto& nested = internal::checked_cast<const NestedType&>(type);
  const auto& fields = nested.fields();
  for (const auto& field : fields) {
    auto it = id_to_field_.try_emplace(field.field_id(), std::cref(field));
    if (!it.second) {
      return InvalidSchema("Duplicate field id found: {}", field.field_id());
    }
    ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*field.type(), this));
  }
  return {};
}

NameToIdVisitor::NameToIdVisitor(
    std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>& name_to_id,
    bool case_sensitive, std::function<std::string(std::string_view)> quoting_func)
    : name_to_id_(name_to_id),
      case_sensitive_(case_sensitive),
      quoting_func_(std::move(quoting_func)) {}

Status NameToIdVisitor::Visit(const ListType& type, const std::string& path,
                              const std::string& short_path) {
  const auto& field = type.fields()[0];
  std::string new_path = BuildPath(path, field.name(), case_sensitive_);
  std::string new_short_path;
  if (field.type()->type_id() == TypeId::kStruct) {
    new_short_path = short_path;
  } else {
    new_short_path = BuildPath(short_path, field.name(), case_sensitive_);
  }
  auto it = name_to_id_.try_emplace(new_path, field.field_id());
  if (!it.second) {
    return InvalidSchema("Duplicate path found: {}, prev id: {}, curr id: {}",
                         it.first->first, it.first->second, field.field_id());
  }
  short_name_to_id_.try_emplace(new_short_path, field.field_id());
  ICEBERG_RETURN_UNEXPECTED(
      VisitTypeInline(*field.type(), this, new_path, new_short_path));
  return {};
}

Status NameToIdVisitor::Visit(const MapType& type, const std::string& path,
                              const std::string& short_path) {
  std::string new_path, new_short_path;
  const auto& fields = type.fields();
  for (const auto& field : fields) {
    new_path = BuildPath(path, field.name(), case_sensitive_);
    if (field.name() == MapType::kValueName &&
        field.type()->type_id() == TypeId::kStruct) {
      new_short_path = short_path;
    } else {
      new_short_path = BuildPath(short_path, field.name(), case_sensitive_);
    }
    auto it = name_to_id_.try_emplace(new_path, field.field_id());
    if (!it.second) {
      return InvalidSchema("Duplicate path found: {}, prev id: {}, curr id: {}",
                           it.first->first, it.first->second, field.field_id());
    }
    short_name_to_id_.try_emplace(new_short_path, field.field_id());
    ICEBERG_RETURN_UNEXPECTED(
        VisitTypeInline(*field.type(), this, new_path, new_short_path));
  }
  return {};
}

Status NameToIdVisitor::Visit(const StructType& type, const std::string& path,
                              const std::string& short_path) {
  const auto& fields = type.fields();
  std::string new_path, new_short_path;
  for (const auto& field : fields) {
    new_path = BuildPath(path, field.name(), case_sensitive_);
    new_short_path = BuildPath(short_path, field.name(), case_sensitive_);
    auto it = name_to_id_.try_emplace(new_path, field.field_id());
    if (!it.second) {
      return InvalidSchema("Duplicate path found: {}, prev id: {}, curr id: {}",
                           it.first->first, it.first->second, field.field_id());
    }
    short_name_to_id_.try_emplace(new_short_path, field.field_id());
    ICEBERG_RETURN_UNEXPECTED(
        VisitTypeInline(*field.type(), this, new_path, new_short_path));
  }
  return {};
}

Status NameToIdVisitor::Visit(const PrimitiveType& type, const std::string& path,
                              const std::string& short_path) {
  return {};
}

std::string NameToIdVisitor::BuildPath(std::string_view prefix,
                                       std::string_view field_name, bool case_sensitive) {
  std::string quoted_name;
  if (!quoting_func_) {
    quoted_name = std::string(field_name);
  } else {
    quoted_name = quoting_func_(field_name);
  }
  if (case_sensitive) {
    return prefix.empty() ? quoted_name : std::string(prefix) + "." + quoted_name;
  }
  return prefix.empty() ? StringUtils::ToLower(quoted_name)
                        : std::string(prefix) + "." + StringUtils::ToLower(quoted_name);
}

void NameToIdVisitor::Finish() {
  for (auto&& it : short_name_to_id_) {
    name_to_id_.try_emplace(it.first, it.second);
  }
}

/// \brief Visitor class for pruning schema columns based on selected field IDs.
///
/// This visitor traverses a schema and creates a projected version containing only
/// the specified fields. It handles different projection modes:
/// - select_full_types=true: Include entire fields when their ID is selected
/// - select_full_types=false: Recursively project nested fields within selected structs
///
/// \warning Error conditions that will cause projection to fail:
/// - Project or Select a Map with just key or value (returns InvalidArgument)
class PruneColumnVisitor {
 public:
  PruneColumnVisitor(const std::unordered_set<int32_t>& selected_ids,
                     bool select_full_types)
      : selected_ids_(selected_ids), select_full_types_(select_full_types) {}

  Result<std::shared_ptr<Type>> Visit(const std::shared_ptr<Type>& type) const {
    switch (type->type_id()) {
      case TypeId::kStruct: {
        auto struct_type = std::static_pointer_cast<StructType>(type);
        return Visit(struct_type);
      }
      case TypeId::kList: {
        auto list_type = std::static_pointer_cast<ListType>(type);
        return Visit(list_type);
      }
      case TypeId::kMap: {
        auto map_type = std::static_pointer_cast<MapType>(type);
        return Visit(map_type);
      }
      default: {
        auto primitive_type = std::static_pointer_cast<PrimitiveType>(type);
        return Visit(primitive_type);
      }
    }
  }

  Result<std::shared_ptr<Type>> Visit(const std::shared_ptr<StructType>& type) const {
    std::vector<std::shared_ptr<Type>> selected_types;
    for (const auto& field : type->fields()) {
      if (select_full_types_ and selected_ids_.contains(field.field_id())) {
        selected_types.emplace_back(field.type());
        continue;
      }
      ICEBERG_ASSIGN_OR_RAISE(auto child_result, Visit(field.type()));
      if (selected_ids_.contains(field.field_id())) {
        selected_types.emplace_back(
            field.type()->is_primitive() ? field.type() : std::move(child_result));
      } else {
        selected_types.emplace_back(std::move(child_result));
      }
    }

    bool same_types = true;
    std::vector<SchemaField> selected_fields;
    const auto& fields = type->fields();
    for (size_t i = 0; i < fields.size(); i++) {
      if (fields[i].type() == selected_types[i]) {
        selected_fields.emplace_back(std::move(fields[i]));
      } else if (selected_types[i]) {
        same_types = false;
        selected_fields.emplace_back(fields[i].field_id(), std::string(fields[i].name()),
                                     std::move(selected_types[i]), fields[i].optional(),
                                     std::string(fields[i].doc()));
      }
    }

    if (!selected_fields.empty()) {
      if (same_types && selected_fields.size() == fields.size()) {
        return type;
      } else {
        return std::make_shared<StructType>(std::move(selected_fields));
      }
    }

    return nullptr;
  }

  Result<std::shared_ptr<Type>> Visit(const std::shared_ptr<ListType>& type) const {
    const auto& element_field = type->fields()[0];
    if (select_full_types_ and selected_ids_.contains(element_field.field_id())) {
      return type;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto child_result, Visit(element_field.type()));

    std::shared_ptr<Type> out;
    if (selected_ids_.contains(element_field.field_id())) {
      if (element_field.type()->is_primitive()) {
        out = std::make_shared<ListType>(element_field);
      } else {
        ICEBERG_ASSIGN_OR_RAISE(out, ProjectList(element_field, std::move(child_result)));
      }
    } else if (child_result) {
      ICEBERG_ASSIGN_OR_RAISE(out, ProjectList(element_field, std::move(child_result)));
    }
    return out;
  }

  Result<std::shared_ptr<Type>> Visit(const std::shared_ptr<MapType>& type) const {
    const auto& key_field = type->fields()[0];
    const auto& value_field = type->fields()[1];

    if (select_full_types_ and selected_ids_.contains(key_field.field_id()) and
        selected_ids_.contains(value_field.field_id())) {
      return type;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto key_result, Visit(key_field.type()));
    ICEBERG_ASSIGN_OR_RAISE(auto value_result, Visit(value_field.type()));

    if (selected_ids_.contains(value_field.field_id()) and
        value_field.type()->is_primitive()) {
      value_result = value_field.type();
    }
    if (selected_ids_.contains(key_field.field_id()) and
        key_field.type()->is_primitive()) {
      key_result = key_field.type();
    }

    if (!key_result && !value_result) {
      return nullptr;
    }

    if (!key_result || !value_result) {
      return InvalidArgument(
          "Cannot project Map with only key or value: key={}, value={}",
          key_result ? "present" : "null", value_result ? "present" : "null");
    }

    ICEBERG_ASSIGN_OR_RAISE(auto out,
                            ProjectMap(key_field, value_field, key_result, value_result));
    return out;
  }

  Result<std::shared_ptr<Type>> Visit(const std::shared_ptr<PrimitiveType>& type) const {
    return nullptr;
  }

  Result<std::shared_ptr<Type>> ProjectList(const SchemaField& element_field,
                                            std::shared_ptr<Type> child_result) const {
    if (!child_result) {
      return nullptr;
    }
    if (element_field.type() == child_result) {
      return std::make_shared<ListType>(element_field);
    }
    return std::make_shared<ListType>(element_field.field_id(), child_result,
                                      element_field.optional());
  }

  Result<std::shared_ptr<Type>> ProjectMap(const SchemaField& key_field,
                                           const SchemaField& value_field,
                                           std::shared_ptr<Type> key_result,
                                           std::shared_ptr<Type> value_result) const {
    SchemaField projected_key_field = key_field;
    if (key_field.type() != key_result) {
      projected_key_field =
          SchemaField(key_field.field_id(), std::string(key_field.name()), key_result,
                      key_field.optional());
    }

    SchemaField projected_value_field = value_field;
    if (value_field.type() != value_result) {
      projected_value_field =
          SchemaField(value_field.field_id(), std::string(value_field.name()),
                      value_result, value_field.optional());
    }

    return std::make_shared<MapType>(projected_key_field, projected_value_field);
  }

 private:
  const std::unordered_set<int32_t>& selected_ids_;
  bool select_full_types_;
};

Result<std::unique_ptr<const Schema>> Schema::Select(std::span<const std::string> names,
                                                     bool case_sensitive) const {
  return SelectInternal(names, case_sensitive);
}

Result<std::unique_ptr<const Schema>> Schema::Select(
    const std::initializer_list<std::string>& names, bool case_sensitive) const {
  return SelectInternal(names, case_sensitive);
}

Result<std::unique_ptr<const Schema>> Schema::SelectInternal(
    std::span<const std::string> names, bool case_sensitive) const {
  const std::string kAllColumns = "*";
  if (std::ranges::find(names, kAllColumns) != names.end()) {
    return std::make_unique<Schema>(*this);
  }

  std::unordered_set<int32_t> selected_ids;
  for (const auto& name : names) {
    ICEBERG_ASSIGN_OR_RAISE(auto result, FindFieldByName(name, case_sensitive));
    if (result.has_value()) {
      selected_ids.insert(result.value().get().field_id());
    }
  }

  PruneColumnVisitor visitor(selected_ids, /*select_full_types=*/true);
  auto self = std::shared_ptr<const StructType>(this, [](const StructType*) {});
  ICEBERG_ASSIGN_OR_RAISE(auto result,
                          visitor.Visit(std::const_pointer_cast<StructType>(self)));

  if (!result) {
    return std::make_unique<Schema>(std::vector<SchemaField>{}, schema_id_);
  }

  if (result->type_id() != TypeId::kStruct) {
    return InvalidSchema("Projected type must be a struct type");
  }

  auto& projected_struct = internal::checked_cast<const StructType&>(*result);

  return FromStructType(std::move(const_cast<StructType&>(projected_struct)), schema_id_);
}

Result<std::unique_ptr<const Schema>> Schema::Project(
    std::unordered_set<int32_t>& field_ids) const {
  PruneColumnVisitor visitor(field_ids, /*select_full_types=*/false);
  auto self = std::shared_ptr<const StructType>(this, [](const StructType*) {});
  ICEBERG_ASSIGN_OR_RAISE(auto result,
                          visitor.Visit(std::const_pointer_cast<StructType>(self)));

  if (!result) {
    return std::make_unique<Schema>(std::vector<SchemaField>{}, schema_id_);
  }

  if (result->type_id() != TypeId::kStruct) {
    return InvalidSchema("Projected type must be a struct type");
  }

  auto& projected_struct = internal::checked_cast<const StructType&>(*result);
  return FromStructType(std::move(const_cast<StructType&>(projected_struct)), schema_id_);
}

}  // namespace iceberg
