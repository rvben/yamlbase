# Fuzz Testing for YamlBase

This directory contains fuzz tests for YamlBase using cargo-fuzz.

## Prerequisites

1. Install the nightly Rust toolchain (required for fuzzing):
```bash
rustup install nightly
```

2. Install cargo-fuzz:
```bash
cargo install cargo-fuzz
```

## Fuzz Targets

We have several fuzz targets to test different components:

1. **fuzz_sql_parser** - Tests SQL query parsing and execution
2. **fuzz_yaml_parser** - Tests YAML parsing for database configuration
3. **fuzz_mysql_protocol** - Tests MySQL protocol packet parsing
4. **fuzz_filter_parser** - Tests WHERE clause expression parsing

## Running Fuzz Tests

To run a specific fuzz target:

```bash
# Run SQL parser fuzzing (uses nightly automatically)
cargo +nightly fuzz run fuzz_sql_parser

# Run with a specific timeout (in seconds)
cargo +nightly fuzz run fuzz_sql_parser -- -max_total_time=60

# Run with more threads
cargo +nightly fuzz run fuzz_sql_parser -- -jobs=4
```

## Analyzing Crashes

If fuzzing finds a crash, it will be saved in `fuzz/artifacts/`. To reproduce:

```bash
# Reproduce a crash
cargo fuzz run fuzz_sql_parser fuzz/artifacts/fuzz_sql_parser/crash-xxxxx

# Minimize a crash input
cargo fuzz tmin fuzz_sql_parser fuzz/artifacts/fuzz_sql_parser/crash-xxxxx
```

## Coverage

To see code coverage achieved by fuzzing:

```bash
cargo fuzz coverage fuzz_sql_parser
```

## Tips

1. Start with short runs (60 seconds) to find obvious bugs
2. Use longer runs (hours/days) to find deeper issues
3. Save interesting inputs in corpus directories for regression testing
4. Consider running different fuzz targets in parallel on different cores