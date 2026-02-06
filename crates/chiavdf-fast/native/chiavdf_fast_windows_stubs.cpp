#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ostream>

#include <gmp.h>

// Keep the mpz ostream overload for MPIR/gmpxx compatibility on Windows.

std::ostream& operator<<(std::ostream& stream, const __mpz_struct* value) {
    if (value == nullptr) {
        return stream << "<null-mpz>";
    }

    char* text = mpz_get_str(nullptr, 10, value);
    if (text == nullptr) {
        return stream;
    }

    stream << text;

    void (*free_fn)(void*, size_t) = nullptr;
    mp_get_memory_functions(nullptr, nullptr, &free_fn);
    if (free_fn != nullptr) {
        free_fn(text, std::strlen(text) + 1);
    } else {
        std::free(text);
    }

    return stream;
}
