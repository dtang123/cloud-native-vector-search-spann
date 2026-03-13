#ifndef _SPTAG_COMMON_INSTRUCTIONUTILS_H_
#define _SPTAG_COMMON_INSTRUCTIONUTILS_H_

#include <string>
#include <vector>
#include <bitset>
#include <array>

#ifndef GPU

#ifndef _MSC_VER
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__)
#include <cpuid.h>
#endif
#if defined(__x86_64__) || defined(_M_X64)
    #include <xmmintrin.h>
    #include <immintrin.h>
#endif

void cpuid(int info[4], int InfoType);

#else
#include <intrin.h>
#define cpuid(info, x)    __cpuidex(info, x, 0)
#endif

#endif

namespace SPTAG {
    namespace COMMON {

        class InstructionSet
        {
            // forward declarations
            class InstructionSet_Internal;

        public:
            // getters
            static bool AVX(void);
            static bool SSE(void);
            static bool SSE2(void);
            static bool AVX2(void);
            static bool AVX512(void);
            static void PrintInstructionSet(void);

        private:
            static const InstructionSet_Internal CPU_Rep;

            class InstructionSet_Internal
            {
            public:
                InstructionSet_Internal();
                bool HW_SSE;
                bool HW_SSE2;
                bool HW_AVX;
                bool HW_AVX2;
                bool HW_AVX512;
            };
        };
    }
}

#endif
