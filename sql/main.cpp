#include "Application.h"
#include "CliHelp.h"
#include "QueryInput.h"

#include <iostream>

int main(int argc, char** argv)
{
    if (sql::CliHelp::should_show(argc, argv))
    {
        sql::CliHelp::write(std::cout, sql::QueryInput::executable_name(argc > 0 ? argv[0] : nullptr));
        return 0;
    }

    sql::Application application;
    return application.run(argc, argv);
}