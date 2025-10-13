# Running the `HashTableTest.serializeDeserializeJoinTable` Test

The hash table serialization round-trip test lives in
[`velox/exec/tests/HashTableTest.cpp`](../velox/exec/tests/HashTableTest.cpp) and
is compiled into the `velox_exec_test` binary. Follow the steps below to build
and run the test.

## 1. Configure and build with tests enabled

Velox uses CMake and defaults to building unit tests. From the repository root
run:

```bash
make release
```

This generates the build files under `_build/release` (using Ninja when
available) and compiles all required targets, including `velox_exec_test`. If
you already have a build directory, you can skip this step or rebuild only the
required target with `cmake --build _build/release --target velox_exec_test`.

## 2. Run the specific test case

Execute the test binary with a GoogleTest filter to focus on the
`serializeDeserializeJoinTable` parameterized case:

```bash
./_build/release/velox/exec/tests/velox_exec_test \
  --gtest_filter=HashTableTest.serializeDeserializeJoinTable
```

GoogleTest will run the test for every parameter instantiation defined in
`HashTableTest`. Add `--gtest_also_run_disabled_tests` if you ever need to run
any disabled cases.

## 3. Optional: run through CTest

Alternatively, you can use CTest from the build directory:

```bash
cd _build/release
ctest --output-on-failure -R velox_exec_test \
  --tests-regex HashTableTest.serializeDeserializeJoinTable
```

This approach integrates with CMake's testing infrastructure and produces the
same results.
