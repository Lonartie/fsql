#include "Application.h"
#include "CliHelp.h"
#include "QueryInput.h"

#include <iostream>

int main(int argc, char** argv)
{
    if (fsql::CliHelp::should_show(argc, argv))
    {
        fsql::CliHelp::write(std::cout, fsql::QueryInput::executable_name(argc > 0 ? argv[0] : nullptr));
        return 0;
    }

    fsql::Application application;
    return application.run(argc, argv);
}