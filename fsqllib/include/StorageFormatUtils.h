#pragma once

#include "SqlData.h"

#include <string>
#include <vector>

namespace fsql::detail
{
    std::string quoted_string_body(const std::string& value);
    std::string serialize_quoted_string_array(const std::vector<std::string>& values);
    std::vector<std::string> parse_quoted_string_array(const std::string& text, const std::string& message);
    std::string xml_escape(const std::string& value);
    std::string xml_unescape(std::string value);
    Table validate_loaded_table(Table table);
}
