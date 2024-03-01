#include <hzncsv.h>

int main(int argc, char *argv[]) {
    hzn::csv::document document;

    hzn::csv::parse_options options;
    const auto result = document.parse_from_file("assets/sample.csv", options);
    if (result != hzn::csv::parse_result::success) {
        return (int)result;
    }

    std::cout << "column count: " << document.column_size() << std::endl;
    std::cout << "row count: " << document.row_size() << std::endl;

    return 0;
}