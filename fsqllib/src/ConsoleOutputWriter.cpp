#include "ConsoleOutputWriter.h"

#include "SerialCoroExecutor.h"

#include <algorithm>
#include <iomanip>
#include <memory>
#include <ostream>
#include <sstream>
#include <vector>

namespace fsql
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

    ConsoleOutputWriter::ConsoleOutputWriter(std::shared_ptr<ICoroExecutor> coro_executor)
        : coro_executor_(std::move(coro_executor))
    {
        if (!coro_executor_)
        {
            coro_executor_ = std::make_shared<SerialCoroExecutor>();
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

            coro_executor_->drive_rows(table.rows(), [&](const Row& row)
            {
                for (std::size_t i = 0; i < row.size(); ++i)
                {
                    widths[i] = std::max(widths[i], row[i].size());
                }
                return true;
            });

            const auto separator = make_separator(widths);
            output << separator << '\n';
            write_row(output, table.column_names, widths);
            output << separator << '\n';

            const auto streamed_row_count = coro_executor_->drive_rows(table.rows(), [&](const Row& row)
            {
                write_row(output, row, widths);
                return true;
            });
            output << separator << '\n';

            if (!result.message.empty())
            {
                output << result.message << '\n';
                return;
            }

            if (result.kind == ExecutionResultKind::Select)
            {
                output << streamed_row_count << " row(s) selected\n";
            }
            return;
        }

        if (!result.message.empty())
        {
            output << result.message << '\n';
        }
    }
}

