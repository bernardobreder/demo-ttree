
#ifndef __TTREE_DEFS_H__
#define __TTREE_DEFS_H__

#include <assert.h>

/**
 * Default number of keys per T*-tree node
 */
#define TTREE_DEFAULT_NUMKEYS 8

/**
 * Minimum allowed number of keys per T*-tree node
 */
#define TNODE_ITEMS_MIN 2

/**
 * Maximum allowed numebr of keys per T*-tree node
 */
#define TNODE_ITEMS_MAX 4096

#define TTREE_ASSERT(cond) assert(cond)

/**
 * @brief Compile time assertion
 */
#define TTREE_CT_ASSERT(cond) \
    ((void)sizeof(char[1 - 2 * !(cond)]))

#ifndef offsetof
#define offsetof(type, field) \
    ((size_t)&(((type *)0)->field) - (size_t)((type *)0))
#endif /* !offsetof */

#if (defined(__cplusplus) || defined(__GNUC__) || defined(__INTEL_COMPILER))
#define __inline inline
#else /* __cplusplus || __GNUC__ || __INTEL_COMPILER  */
#define __inline
#endif /* !__cplusplus && !__GNUC__ && !__INTEL_COMPILER */

#ifdef __GNUC__
#if (__GNUC__ >= 3)
#define LIKELY(cond)   __builtin_expect((cond), 1)
#define UNLIKELY(cond) __builtin_expect((cond), 0)
#else /* __GNUC__ >= 3 */
#define LIKELY(cond)   cond
#define UNLIKELY(cond) cond
#endif /* __GNUC__ < 3 */
#endif /*__GNUC__ */

#endif /* !__TTREE_DEFS_H__ */
