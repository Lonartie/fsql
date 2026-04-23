#include "ConsoleOutputWriter.h"

#include <algorithm>
#include <iomanip>
#include <ostream>
#include <sstream>
#include <vector>

namespace sql
{
    namespace
    {
        std::string make_separator(const std::vector<std::size_t>& widths)
        {
            std::ostringstream stream;
            stream << '+';
            for (const auto width : widths)
            {
                stream << std::string(width + 2, '-') << '+';
            }
            return stream.str();
        }

        void write_row(std::ostream& output, const std::vector<std::string>& values, const std::vector<std::size_t>& widths)
        {
            output << '|';
            for (std::size_t i = 0; i < values.size(); ++i)
            {
                output << ' ' << std::left << std::setw(static_cast<int>(widths[i])) << values[i] << ' ' << '|';
            }
            output << '\n';
        }
    }

    void ConsoleOutputWriter::write(std::ostream& output, const ExecutionResult& result) const
    {
        if (!result.success)
        {
            return;
        }

        if (result.table.has_value())
        {
            const auto& table = *result.table;
            std::vector<std::size_t> widths(table.column_names.size(), 0);
            for (std::size_t i = 0; i < table.column_names.size(); ++i)
            {
                widths[i] = table.column_names[i].size();
            }
            for (const auto& row : table.rows)
            {
                for (std::size_t i = 0; i < row.size(); ++i)
                {
                    widths[i] = std::max(widths[i], row[i].size());
                }
            }

            const auto separator = make_separator(widths);
            output << separator << '\n';
            write_row(output, table.column_names, widths);
            output << separator << '\n';
            for (const auto& row : table.rows)
            {
                write_row(output, row, widths);
            }
            output << separator << '\n';
        }

        if (!result.message.empty())
        {
            output << result.message << '\n';
        }
    }
}

