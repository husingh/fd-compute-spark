// ---------------------------------------------------------------------------
// test_apply_env_overrides.cpp
//
// Unit tests for the applyEnvOverrides() helper that is used in both
// stack_distance_map_jni.cpp and stack_distance_reduce_jni.cpp.
//
// Build & run (from fd-compute-spark/cpp_src/tests/):
//   g++ -std=c++17 -o test_apply_env_overrides test_apply_env_overrides.cpp && \
//   ./test_apply_env_overrides
//
// No external test framework needed — uses a minimal hand-rolled harness.
// ---------------------------------------------------------------------------

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <string>

// ---------------------------------------------------------------------------
// Copy of applyEnvOverrides() — kept in sync with the JNI files.
// (In production it lives as a static inside each JNI translation unit.)
// ---------------------------------------------------------------------------
static void applyEnvOverrides(const std::string &overrides) {
    if (overrides.empty()) return ;
    char *buf  = strdup(overrides.c_str()) ;
    char *line = strtok(buf, "\n") ;
    while (line) {
        char *eq = strchr(line, '=') ;
        if (eq) {
            *eq = '\0' ;
            setenv(line, eq + 1, /*overwrite=*/1) ;
            *eq = '=' ;
        }
        line = strtok(NULL, "\n") ;
    }
    free(buf) ;
}

// ---------------------------------------------------------------------------
// Minimal test harness
// ---------------------------------------------------------------------------
static int g_tests_run    = 0 ;
static int g_tests_passed = 0 ;

#define TEST(name) void name() ; struct _Reg_##name { _Reg_##name() { run_test(#name, name); } } _reg_##name ; void name()

static void run_test(const char *name, void (*fn)()) {
    g_tests_run++ ;
    printf("  %-60s ", name) ;
    fflush(stdout) ;
    try {
        fn() ;
        g_tests_passed++ ;
        printf("PASS\n") ;
    } catch (const std::exception &e) {
        printf("FAIL: %s\n", e.what()) ;
    } catch (...) {
        printf("FAIL (unknown exception)\n") ;
    }
}

#define ASSERT_STREQ(a, b) do { \
    const char *_a = (a) ; const char *_b = (b) ; \
    if (_a == nullptr || _b == nullptr || strcmp(_a, _b) != 0) { \
        char _msg[512] ; \
        snprintf(_msg, sizeof(_msg), "ASSERT_STREQ failed:\n  expected: \"%s\"\n  got:      \"%s\"\n  at line %d", \
                 _b ? _b : "(null)", _a ? _a : "(null)", __LINE__) ; \
        throw std::runtime_error(_msg) ; \
    } \
} while(0)

#define ASSERT_NULL(a) do { \
    if ((a) != nullptr) { \
        char _msg[256] ; \
        snprintf(_msg, sizeof(_msg), "ASSERT_NULL failed: expected null at line %d", __LINE__) ; \
        throw std::runtime_error(_msg) ; \
    } \
} while(0)

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Helper: unset a var before each test that uses it
static void unset(const char *k) { unsetenv(k) ; }

TEST(empty_string_sets_nothing) {
    unset("_FD_TEST_A") ;
    applyEnvOverrides("") ;
    ASSERT_NULL(getenv("_FD_TEST_A")) ;
}

TEST(single_key_value_pair) {
    unset("_FD_TEST_A") ;
    applyEnvOverrides("_FD_TEST_A=/tmp/vcds.txt") ;
    ASSERT_STREQ(getenv("_FD_TEST_A"), "/tmp/vcds.txt") ;
    unset("_FD_TEST_A") ;
}

TEST(two_pairs_separated_by_newline) {
    unset("_FD_TEST_A") ; unset("_FD_TEST_B") ;
    applyEnvOverrides("_FD_TEST_A=/tmp/a\n_FD_TEST_B=/tmp/b") ;
    ASSERT_STREQ(getenv("_FD_TEST_A"), "/tmp/a") ;
    ASSERT_STREQ(getenv("_FD_TEST_B"), "/tmp/b") ;
    unset("_FD_TEST_A") ; unset("_FD_TEST_B") ;
}

TEST(trailing_newline_is_harmless) {
    unset("_FD_TEST_A") ;
    applyEnvOverrides("_FD_TEST_A=/tmp/vcds\n") ;
    ASSERT_STREQ(getenv("_FD_TEST_A"), "/tmp/vcds") ;
    unset("_FD_TEST_A") ;
}

TEST(value_with_path_containing_slashes) {
    unset("_FD_TEST_A") ;
    applyEnvOverrides("_FD_TEST_A=/very/deep/path/to/vcds.txt") ;
    ASSERT_STREQ(getenv("_FD_TEST_A"), "/very/deep/path/to/vcds.txt") ;
    unset("_FD_TEST_A") ;
}

TEST(key_index_suffix_stripped_in_value) {
    // The local filename already has .keyN stripped by Scala before being
    // passed here — verify a plain path (without .key) is stored as-is.
    unset("_FD_TEST_A") ;
    applyEnvOverrides("_FD_TEST_A=/tmp/fd_cfg_97/vcds.txt") ;
    ASSERT_STREQ(getenv("_FD_TEST_A"), "/tmp/fd_cfg_97/vcds.txt") ;
    unset("_FD_TEST_A") ;
}

TEST(overwrite_existing_env_var) {
    // If the var was previously set (e.g. by run_pipeline_job.sh to an S3 URL),
    // applyEnvOverrides must overwrite it with the local path.
    setenv("_FD_TEST_A", "s3://bucket/vcds.txt.key2", 1) ;
    applyEnvOverrides("_FD_TEST_A=/tmp/vcds.txt") ;
    ASSERT_STREQ(getenv("_FD_TEST_A"), "/tmp/vcds.txt") ;
    unset("_FD_TEST_A") ;
}

TEST(all_seven_known_file_vars) {
    const char *vars[] = {
        "FD_MAPREDUCE_VCDS_FILE",
        "FD_MAPREDUCE_CPCODES_FILE",
        "FD_MAPREDUCE_REGIONS_FILE",
        "FD_MAPREDUCE_GHOSTIP_FILE",
        "FD_MAPREDUCE_CPCODE_MAP_FILE",
        "FD_MAPREDUCE_VCD_MAP_NETWORK_FILE",
        "FD_MAPREDUCE_ARL_VCD_TRANSLATION_FILE"
    } ;
    for (const char *v : vars) unset(v) ;

    std::string overrides ;
    for (int i = 0; i < 7; i++) {
        if (i > 0) overrides += "\n" ;
        overrides += std::string(vars[i]) + "=/tmp/file" + std::to_string(i) ;
    }
    applyEnvOverrides(overrides) ;

    for (int i = 0; i < 7; i++) {
        std::string expected = "/tmp/file" + std::to_string(i) ;
        ASSERT_STREQ(getenv(vars[i]), expected.c_str()) ;
        unset(vars[i]) ;
    }
}

TEST(line_without_equals_is_skipped) {
    // A malformed line (no '=') must not crash or set anything unexpected.
    unset("_FD_TEST_A") ;
    unset("BADLINE") ;
    applyEnvOverrides("BADLINE\n_FD_TEST_A=/tmp/ok") ;
    ASSERT_NULL(getenv("BADLINE")) ;
    ASSERT_STREQ(getenv("_FD_TEST_A"), "/tmp/ok") ;
    unset("_FD_TEST_A") ;
}

TEST(original_string_is_not_mutated) {
    // strdup() must be used internally — the input std::string stays intact.
    std::string input = "FD_MAPREDUCE_VCDS_FILE=/tmp/vcds.txt" ;
    std::string copy  = input ;
    applyEnvOverrides(input) ;
    if (input != copy) throw std::runtime_error("input string was mutated") ;
    unset("FD_MAPREDUCE_VCDS_FILE") ;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main() {
    printf("\n=== applyEnvOverrides tests ===\n\n") ;
    // Tests self-register via static initializers above.
    // All have already run by the time we get here.
    printf("\n%d / %d tests passed.\n\n", g_tests_passed, g_tests_run) ;
    return (g_tests_passed == g_tests_run) ? 0 : 1 ;
}
