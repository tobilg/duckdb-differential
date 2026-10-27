#include "merge_types.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/error_data.hpp"

namespace duckdb {
namespace {
string Quote(const string &text, char quote = '\'') {
    string result(1, quote);
    for (auto c : text) { result += c; if (c == quote) result += c; }
    return result + quote;
}
[[noreturn]] void UnsupportedType(const string &reason) {
    throw InvalidInputException("[differential:merge_unsupported] %s", reason);
}
// Keep the original scalar fields in place. New payloads use their actual native
// types: in particular no timestamp conversion, unsigned narrowing, or text codec.
child_list_t<LogicalType> ScalarFields() {
    return {{"name", LogicalType::VARCHAR}, {"sql_type", LogicalType::VARCHAR},
            {"is_null", LogicalType::BOOLEAN}, {"boolean_value", LogicalType::BOOLEAN},
            {"integer_value", LogicalType::HUGEINT}, {"text_value", LogicalType::VARCHAR},
            {"decimal_unscaled", LogicalType::HUGEINT}, {"decimal_width", LogicalType::UTINYINT},
            {"decimal_scale", LogicalType::UTINYINT}, {"date_value", LogicalType::DATE},
            {"timestamp_value", LogicalType::TIMESTAMP}, {"unsigned_integer_value", LogicalType::UHUGEINT},
            {"float_value", LogicalType::FLOAT}, {"double_value", LogicalType::DOUBLE},
            {"uuid_value", LogicalType::UUID}, {"blob_value", LogicalType::BLOB},
            {"time_value", LogicalType::TIME}, {"time_ns_value", LogicalType::TIME_NS},
            {"time_tz_value", LogicalType::TIME_TZ}, {"timestamp_s_value", LogicalType::TIMESTAMP_S},
            {"timestamp_ms_value", LogicalType::TIMESTAMP_MS}, {"timestamp_ns_value", LogicalType::TIMESTAMP_NS},
            {"timestamp_tz_value", LogicalType::TIMESTAMP_TZ},
            {"timestamp_tz_ns_value", LogicalType::TIMESTAMP_TZ_NS}, {"interval_value", LogicalType::INTERVAL}};
}
const LogicalType &ScalarType() {
    static const auto type = LogicalType::STRUCT(ScalarFields());
    return type;
}
const LogicalType &NodeType() {
    static const auto type = LogicalType::STRUCT({{"parent", LogicalType::BIGINT}, {"position", LogicalType::BIGINT},
                                                  {"cell", ScalarType()}});
    return type;
}
void CheckType(const LogicalType &type, idx_t depth, idx_t &nodes) {
    if (depth > 32 || ++nodes > 1024)
        throw IOException("[differential:merge_limit] Type exceeds 32 nested levels or 1024 type nodes");
    if (!type.GetAlias().empty()) UnsupportedType("custom/aliased type: " + type.ToString());
    switch (type.id()) {
    case LogicalTypeId::BOOLEAN:
    case LogicalTypeId::TINYINT: case LogicalTypeId::SMALLINT: case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT: case LogicalTypeId::HUGEINT:
    case LogicalTypeId::UTINYINT: case LogicalTypeId::USMALLINT: case LogicalTypeId::UINTEGER:
    case LogicalTypeId::UBIGINT: case LogicalTypeId::UHUGEINT:
    case LogicalTypeId::FLOAT: case LogicalTypeId::DOUBLE: case LogicalTypeId::DECIMAL:
    case LogicalTypeId::UUID: case LogicalTypeId::BLOB: case LogicalTypeId::DATE:
    case LogicalTypeId::TIME: case LogicalTypeId::TIME_NS: case LogicalTypeId::TIME_TZ:
    case LogicalTypeId::TIMESTAMP_SEC: case LogicalTypeId::TIMESTAMP_MS: case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::TIMESTAMP_NS: case LogicalTypeId::TIMESTAMP_TZ: case LogicalTypeId::TIMESTAMP_TZ_NS:
    case LogicalTypeId::INTERVAL:
        return;
    case LogicalTypeId::VARCHAR:
        if (!StringType::GetCollation(type).empty()) UnsupportedType("non-default collation");
        return;
    case LogicalTypeId::STRUCT: case LogicalTypeId::TUPLE:
        for (auto &child : StructType::GetChildTypes(type)) CheckType(child.second, depth + 1, nodes);
        return;
    case LogicalTypeId::LIST: CheckType(ListType::GetChildType(type), depth + 1, nodes); return;
    case LogicalTypeId::ARRAY: CheckType(ArrayType::GetChildType(type), depth + 1, nodes); return;
    case LogicalTypeId::MAP:
        CheckType(MapType::KeyType(type), depth + 1, nodes);
        CheckType(MapType::ValueType(type), depth + 1, nodes);
        return;
    case LogicalTypeId::UNION:
        for (idx_t i = 0; i < UnionType::GetMemberCount(type); i++)
            CheckType(UnionType::GetMemberType(type, i), depth + 1, nodes);
        return;
    default: UnsupportedType("type " + type.ToString());
    }
}
vector<Value> ScalarValues(const string &name, const LogicalType &type, const Value &value) {
    vector<Value> fields;
    for (auto &field : StructType::GetChildTypes(ScalarType())) fields.emplace_back(field.second);
    fields[0] = Value(name); fields[1] = Value(type.ToString()); fields[2] = Value(value.IsNull());
    if (type.id() == LogicalTypeId::DECIMAL) {
        fields[7] = Value::UTINYINT(DecimalType::GetWidth(type));
        fields[8] = Value::UTINYINT(DecimalType::GetScale(type));
        if (!value.IsNull()) {
            hugeint_t n;
            switch (type.InternalType()) {
            case PhysicalType::INT16: n = hugeint_t(value.GetValueUnsafe<int16_t>()); break;
            case PhysicalType::INT32: n = hugeint_t(value.GetValueUnsafe<int32_t>()); break;
            case PhysicalType::INT64: n = hugeint_t(value.GetValueUnsafe<int64_t>()); break;
            default: n = value.GetValueUnsafe<hugeint_t>(); break;
            }
            fields[6] = Value::HUGEINT(n);
        }
    } else if (!value.IsNull() && !MergeNestedType(type)) {
        idx_t field;
        switch (type.id()) {
        case LogicalTypeId::BOOLEAN: field = 3; break;
        case LogicalTypeId::VARCHAR: field = 5; break;
        case LogicalTypeId::DATE: field = 9; break;
        case LogicalTypeId::TIMESTAMP: field = 10; break;
        case LogicalTypeId::UHUGEINT: field = 11; break;
        case LogicalTypeId::FLOAT: field = 12; break;
        case LogicalTypeId::DOUBLE: field = 13; break;
        case LogicalTypeId::UUID: field = 14; break;
        case LogicalTypeId::BLOB: field = 15; break;
        case LogicalTypeId::TIME: field = 16; break;
        case LogicalTypeId::TIME_NS: field = 17; break;
        case LogicalTypeId::TIME_TZ: field = 18; break;
        case LogicalTypeId::TIMESTAMP_SEC: field = 19; break;
        case LogicalTypeId::TIMESTAMP_MS: field = 20; break;
        case LogicalTypeId::TIMESTAMP_NS: field = 21; break;
        case LogicalTypeId::TIMESTAMP_TZ: field = 22; break;
        case LogicalTypeId::TIMESTAMP_TZ_NS: field = 23; break;
        case LogicalTypeId::INTERVAL: field = 24; break;
        default: fields[4] = value.DefaultCastAs(LogicalType::HUGEINT); return fields;
        }
        fields[field] = value;
    }
    return fields;
}
// A flat, typed tree avoids a recursive SQL return type or a lossy JSON/text
// conversion. Indexes and parents are one-based; root parent/position are zero.
void AppendNode(vector<Value> &nodes, int64_t parent, int64_t position, const string &name, const Value &value) {
    auto &type = value.type();
    auto index = int64_t(nodes.size() + 1);
    nodes.push_back(Value::STRUCT(NodeType(), {Value::BIGINT(parent), Value::BIGINT(position),
        Value::STRUCT(ScalarType(), ScalarValues(name, type, value))}));
    if (value.IsNull()) return;
    const vector<Value> *children = nullptr;
    switch (type.id()) {
    case LogicalTypeId::STRUCT: case LogicalTypeId::TUPLE: children = &StructValue::GetChildren(value); break;
    case LogicalTypeId::LIST: children = &ListValue::GetChildren(value); break;
    case LogicalTypeId::ARRAY: children = &ArrayValue::GetChildren(value); break;
    case LogicalTypeId::MAP: children = &MapValue::GetChildren(value); break;
    case LogicalTypeId::UNION: {
        auto tag = UnionValue::GetTag(value);
        AppendNode(nodes, index, tag + 1, UnionType::GetMemberName(type, tag).GetIdentifierName(), UnionValue::GetValue(value));
        return;
    }
    default: return;
    }
    for (idx_t i = 0; i < children->size(); i++) {
        auto child_name = type.id() == LogicalTypeId::STRUCT ? StructType::GetChildName(type, i).GetIdentifierName() : "";
        AppendNode(nodes, index, i + 1, child_name, (*children)[i]);
    }
}
// Count each nested node, including NULL/empty containers, before retrieving any
// Values into C++. DuckDB evaluates aggregates/lambdas and may spill normally.
string PayloadBytes(const LogicalType &type, const string &e, idx_t depth) {
    vector<string> children;
    switch (type.id()) {
    case LogicalTypeId::VARCHAR: return "coalesce(strlen(" + e + "),0)";
    case LogicalTypeId::BLOB: return "coalesce(octet_length(" + e + "),0)";
    case LogicalTypeId::STRUCT: case LogicalTypeId::TUPLE:
        for (idx_t i = 0; i < StructType::GetChildCount(type); i++) {
            auto name = StructType::GetChildName(type, i).GetIdentifierName();
            auto key = type.id() == LogicalTypeId::TUPLE ? to_string(i + 1) : Quote(name);
            auto &child = StructType::GetChildType(type, i);
            children.push_back(to_string(512 + name.size() + child.ToString().size()) + "+" +
                               PayloadBytes(child, "struct_extract(" + e + "," + key + ")", depth + 1));
        }
        break;
    case LogicalTypeId::UNION:
        for (idx_t i = 0; i < UnionType::GetMemberCount(type); i++) {
            auto name = UnionType::GetMemberName(type, i).GetIdentifierName();
            auto &child = UnionType::GetMemberType(type, i);
            children.push_back("CASE WHEN union_tag(" + e + ")=" + Quote(name) + " THEN " +
                to_string(512 + name.size() + child.ToString().size()) + "+" +
                PayloadBytes(child, "union_extract(" + e + "," + Quote(name) + ")", depth + 1) + " ELSE 0 END");
        }
        break;
    case LogicalTypeId::LIST: case LogicalTypeId::ARRAY: case LogicalTypeId::MAP: {
        auto child = type.id() == LogicalTypeId::ARRAY ? ArrayType::GetChildType(type) : ListType::GetChildType(type);
        auto input = type.id() == LogicalTypeId::MAP ? "map_entries(" + e + ")" : e;
        auto variable = "__merge_size_" + to_string(depth);
        children.push_back("coalesce(list_sum(list_transform(" + input + ",lambda " + variable + ": " +
            to_string(512 + child.ToString().size()) + "+" + PayloadBytes(child, variable, depth + 1) + ")),0)");
        break;
    }
    default: return "0::BIGINT";
    }
    auto overhead = to_string(512 + type.ToString().size()) + "::BIGINT";
    return "CASE WHEN " + e + " IS NULL THEN " + overhead + " ELSE " + overhead + "+" +
           (children.empty() ? "0" : StringUtil::Join(children, "+")) + " END";
}
} // namespace

bool MergeNestedType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::STRUCT: case LogicalTypeId::TUPLE: case LogicalTypeId::LIST:
    case LogicalTypeId::ARRAY: case LogicalTypeId::MAP: case LogicalTypeId::UNION: return true;
    default: return false;
    }
}
void CheckMergeType(const LogicalType &type) {
    idx_t nodes = 0;
    try { CheckType(type, 0, nodes); }
    catch (const InvalidInputException &error) {
        UnsupportedType("column type " + type.ToString() + ": " + ErrorData(error).RawMessage());
    }
}
LogicalType MergeCellType() {
    static const auto type = [] {
        auto fields = ScalarFields();
        fields.emplace_back("nested_value", LogicalType::LIST(NodeType()));
        return LogicalType::STRUCT(std::move(fields));
    }();
    return type;
}
Value MergeCell(const string &name, const LogicalType &type, const Value &value) {
    auto fields = ScalarValues(name, type, value);
    vector<Value> nodes;
    if (MergeNestedType(type)) AppendNode(nodes, 0, 0, name, value);
    fields.push_back(Value::LIST(NodeType(), std::move(nodes)));
    return Value::STRUCT(MergeCellType(), std::move(fields));
}
string MergeValueBytesSQL(const LogicalType &type, const string &expression) {
    return "(" + PayloadBytes(type, expression, 0) + ")";
}
} // namespace duckdb
