#include <iostream>
#include <string>
#include <vector>

#include "CsvStorage.h"
#include "Executor.h"
#include "MemoryStorage.h"
#include "Parser.h"
#include "SerialCoroExecutor.h"
#include "Tokenizer.h"

std::vector<std::string> get_names() {
  return {"Alice",   "Bob",      "Charlie", "David",   "Eve",       "Frank",
          "Grace",   "Heidi",    "Ivan",    "Judy",    "Kevin",     "Leo",
          "Mallory", "Nina",     "Oscar",   "Peggy",   "Quentin",   "Ruth",
          "Sam",     "Trudy",    "Uma",     "Victor",  "Walter",    "Xavier",
          "Yvonne",  "Zoe",      "Adam",    "Brian",   "Catherine", "Derek",
          "Emily",   "Franklin", "Gina",    "Hank",    "Isabel",    "Jack",
          "Karen",   "Liam",     "Mia",     "Nathan",  "Olivia",    "Paul",
          "Quincy",  "Rachel",   "Steven",  "Tina",    "Ulysses",   "Vanessa",
          "Wendy",   "Xander",   "Yara",    "Zach",    "Aaron",     "Beth",
          "Carl",    "Diana",    "Ethan",   "Fiona",   "George",    "Hannah",
          "Ian",     "Julia",    "Kyle",    "Laura",   "Michael",   "Nora",
          "Oscar",   "Pamela",   "Quinn",   "Rebecca", "Sean",      "Tara",
          "Umar",    "Violet",   "Will",    "Xenia",   "Yusuf",     "Zara"};
}

std::vector<std::string> get_cities() {
  return {"New York", "Los Angeles",  "Chicago",     "Houston",
          "Phoenix",  "Philadelphia", "San Antonio", "San Diego",
          "Dallas",   "San Jose",     "Austin",      "Jacksonville"};
}

std::vector<std::string> get_countries() {
  return {"USA",   "Canada",    "UK",     "Germany", "France", "Italy",
          "Spain", "Australia", "Brazil", "India",   "China",  "Japan"};
}

std::vector<std::string> get_random_row() {
  static std::vector<std::string> names = get_names();
  static std::vector<std::string> cities = get_cities();
  static std::vector<std::string> countries = get_countries();

  std::string name = names[rand() % names.size()];
  std::string city = cities[rand() % cities.size()];
  std::string country = countries[rand() % countries.size()];

  return {name, city, country};
}

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <number_rows>" << std::endl;
    return 1;
  }

  std::srand(std::time(nullptr));
  size_t row_count = std::stoull(std::string(argv[1]));

  auto storage = std::make_shared<fsql::MemoryStorage>();
  auto coro_executor = std::make_shared<fsql::SerialCoroExecutor>();
  auto executor = fsql::Executor(storage, coro_executor);
  auto parse = [](const std::string &query) {
    fsql::Tokenizer tokenizer(query);
    fsql::Parser parser(tokenizer.tokenize());
    return parser.parse_statement();
  };

  if (!executor
           .execute(parse(
               "CREATE TABLE gen (id AUTO_INCREMENT, name, city, country);"))
           .success) {
    std::cerr << "Failed to create table" << std::endl;
    return 1;
  }

  for (size_t i = 0; i < row_count; ++i) {
    auto row = get_random_row();
    if (!executor
             .execute(parse("INSERT INTO gen (name, city, country) VALUES ('" +
                            row[0] + "', '" + row[1] + "', '" + row[2] + "');"))
             .success) {
      std::cerr << "Failed to insert row: " << row[0] << ", " << row[1] << ", "
                << row[2] << std::endl;
      return 1;
    }
    std::cout << "Inserted row id: " << i + 1 << "\n";
  }

  auto csv = fsql::CsvStorage();
  csv.save_table(storage->load_table("gen"));
  std::cout << "Successfully inserted " << row_count << " rows into the 'gen' table." << std::endl;
}