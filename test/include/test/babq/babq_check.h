#ifndef BABQ_TEST_BABQ_CHECK_H
#define BABQ_TEST_BABQ_CHECK_H

#include <cstdio>
#include <cstdlib>

namespace babq_test {

[[noreturn]] inline void check_fail(const char *expr, const char *file, int line,
                                    const char *func) {
  std::fprintf(stderr,
               "\n[BABQ CHECK FAILED] %s\n"
               "                 IN: %s:%d:%s\n",
               expr, file, line, func);
  std::fflush(stderr);
  std::abort();
}

template <typename A, typename B>
[[noreturn]] inline void check_eq_fail(const char *lhs_expr, const char *rhs_expr,
                                       A lhs, B rhs, const char *file, int line,
                                       const char *func) {
  std::fprintf(stderr,
               "\n[BABQ CHECK FAILED] %s == %s\n"
               "                lhs = %llu\n"
               "                rhs = %llu\n"
               "                 IN: %s:%d:%s\n",
               lhs_expr, rhs_expr, static_cast<unsigned long long>(lhs),
               static_cast<unsigned long long>(rhs), file, line, func);
  std::fflush(stderr);
  std::abort();
}

} 

#define BABQ_CHECK(cond)                                                       \
  do {                                                                         \
    if (!(cond)) {                                                             \
      ::babq_test::check_fail(#cond, __FILE__, __LINE__, __func__);            \
    }                                                                          \
  } while (0)

#define BABQ_CHECK_EQ(lhs, rhs)                                                \
  do {                                                                         \
    const auto babq_check_lhs_ = (lhs);                                        \
    const auto babq_check_rhs_ = (rhs);                                        \
    if (!(babq_check_lhs_ == babq_check_rhs_)) {                               \
      ::babq_test::check_eq_fail(#lhs, #rhs, babq_check_lhs_, babq_check_rhs_, \
                                 __FILE__, __LINE__, __func__);                \
    }                                                                          \
  } while (0)

#endif // BABQ_TEST_BABQ_CHECK_H
