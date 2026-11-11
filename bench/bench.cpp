#include "duckdb.hpp"

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>

int main(int argc, char **argv) {
  try {
    if (argc != 6) throw std::runtime_error("usage: DB SQL_FILE THREADS WARMUPS RUNS");
    const std::string database = argv[1];
    std::ifstream source(argv[2]);
    if (!source) throw std::runtime_error("cannot read SQL file");
    const std::string sql((std::istreambuf_iterator<char>(source)), std::istreambuf_iterator<char>());
    const int threads = std::stoi(argv[3]);
    const int warmups = std::stoi(argv[4]);
    const int runs = std::stoi(argv[5]);
    if (threads < 1 || warmups < 0 || runs < 1) throw std::runtime_error("invalid arguments");
    duckdb::DBConfig config;
    config.options.access_mode = duckdb::AccessMode::READ_ONLY;
    duckdb::DuckDB db(database, &config);
    duckdb::Connection con(db);
    auto setting = con.Query("SET threads=" + std::to_string(threads));
    if (!setting || setting->HasError()) throw std::runtime_error(setting ? setting->GetError() : "SET failed");
    std::cout << "iteration,ms,groups,ordered_payload_hash\n";
    for (int iteration = -warmups; iteration < runs; ++iteration) {
      const auto begin = std::chrono::steady_clock::now();
      auto result = con.Query(sql);
      if (!result || result->HasError()) throw std::runtime_error(result ? result->GetError() : "query failed");
      auto chunk = result->Fetch();
      if (!chunk || chunk->size() != 1 || chunk->ColumnCount() != 2)
        throw std::runtime_error("unexpected result shape");
      const auto elapsed = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - begin).count();
      const auto groups = chunk->GetValue(0, 0).ToString();
      const auto hash = chunk->GetValue(1, 0).ToString();
      if (iteration >= 0) {
        std::cout << iteration << ',' << std::fixed << std::setprecision(6) << elapsed << ','
                  << groups << ',' << hash << '\n';
      }
    }
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "bench: " << error.what() << '\n';
    return 1;
  }
}
