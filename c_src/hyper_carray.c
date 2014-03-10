// Copyright (c) 2014 Johannes Huning <mail@johanneshuning.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

#include <string.h>
#include <stdbool.h>
#include <tgmath.h>

#include "erl_nif.h"


/// Struct placed in front of the array for convenience and less repetition.
struct hyper_carray {
    /// Length = cells in the array = bytes allocated.
    unsigned int length;
    /// The array itself, 2^precision cells.
    char* bytes;
};


/// Erlang NIF resource instance for our header+array combination.
static ErlNifResourceType* CARRAY_RESOURCE;


// Convenience macro for all NIFs reading the resource from the env.
#ifndef CARRAY_ENIF_GET
#define CARRAY_ENIF_GET(_term, _varname) \
    struct hyper_carray* _varname = NULL; \
    void* _varname_res = NULL; \
    if (!enif_get_resource(env, _term, CARRAY_RESOURCE, &_varname_res)) \
        return enif_make_badarg(env); \
    _varname = _varname_res;
#endif



/** Given the desired precision, allocate a new hyper_carray and make 'arr'
    point to it. Does not zero-out the array itself. */
static bool
carray_alloc(unsigned int precision, struct hyper_carray** arr)
{
    unsigned int length = pow(2, precision);
    size_t struct_size = sizeof(struct hyper_carray);
    size_t alloc_size = struct_size + length;

    void* res = enif_alloc_resource(CARRAY_RESOURCE, alloc_size);
    memset(res, 0, struct_size);

    *arr = res;
    (*arr)->length = length;
    (*arr)->bytes = res + sizeof(struct hyper_carray);

    return true;
}


/** Obtain a new hyper_carray register. */
static ERL_NIF_TERM
new_register(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int precision = 0;
    if (!enif_get_int(env, argv[0], &precision))
        return enif_make_badarg(env);

    struct hyper_carray* arr = NULL;
    carray_alloc(precision, &arr);
    memset(arr->bytes, 0, arr->length);

    ERL_NIF_TERM erl_res = enif_make_resource(env, arr);
    enif_release_resource(arr);
    return erl_res;
}


/// Please see src/hyper_register.erl.
static ERL_NIF_TERM
set(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int index = 0;
    int value = 0;
    if (!enif_get_int(env, argv[0], &index) ||
            !enif_get_int(env, argv[1], &value))
        return enif_make_badarg(env);

    CARRAY_ENIF_GET(argv[2], arr);

    // Validate bounds.
    if (index > arr->length - 1)
        return enif_make_badarg(env);

    // Update cell if given value is bigger.
    char* cell = arr->bytes + index;
    if (value > *cell)
        *cell = value;

    return enif_make_resource(env, arr);
}


/// Please see src/hyper_register.erl.
static ERL_NIF_TERM
max_merge(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    unsigned int nregs = 0;
    if (!enif_get_list_length(env, argv[0], &nregs))
        return enif_make_badarg(env);

    // Require at least two entries in the list.
    if(nregs < 2)
        return enif_make_badarg(env);

    bool have_base = false;
    ERL_NIF_TERM head;
    ERL_NIF_TERM tail = argv[0];
    struct hyper_carray* newreg = NULL;
    struct hyper_carray* curr = NULL;

    for (int i = 0; i < nregs; ++i) {
        // Get next item.
        if (!enif_get_list_cell(env, tail, &head, &tail))
            goto make_badarg; // Release and badarg.

        // Unpack register from item.
        void* res = NULL;
        if (!enif_get_resource(env, head, CARRAY_RESOURCE, &res))
            goto make_badarg; // Release and badarg.

        curr = res;

        // Create the new register once.
        if (!have_base) {
            unsigned int precision = log2(curr->length);
            carray_alloc(precision, &newreg);
            memset(newreg->bytes, 0, newreg->length);
            have_base = true;
        }
        else {
            // For all other registers, require the length to be uniform.
            if (curr->length != newreg->length) {

// Release whatever we have allocated and then badarg.s
make_badarg:    if (have_base)
                    enif_release_resource(newreg);
                return enif_make_badarg(env);
            }
        }

        char* cell = NULL;
        unsigned short currval = 0;
        unsigned int ncells = newreg->length;

        for (int j = 0; j < ncells; ++j) {
            cell = newreg->bytes + j;
            currval = *(curr->bytes + j);

            if (currval > *cell)
                *cell = currval;
        }
    }

    return enif_make_resource(env, newreg);
}


/** Return the total number of bytes allocated,
    disregarding the fixed-size header. */
static ERL_NIF_TERM
bytes(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    CARRAY_ENIF_GET(argv[0], arr);
    return enif_make_int(env, arr->length);
}


/// Please see src/hyper_register.erl.
static ERL_NIF_TERM
register_sum(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    CARRAY_ENIF_GET(argv[0], arr);

    unsigned int length = arr->length;
    double sum = 0.0;
    int curr = 0;
    int i = 0;

    for (; i < length; ++i) {
        curr = *(arr->bytes + i);
        sum += pow(2, -curr);
    }

    return enif_make_double(env, sum);
}


/// Please see src/hyper_register.erl.
static ERL_NIF_TERM
zero_count(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    CARRAY_ENIF_GET(argv[0], arr);

    unsigned int length = arr->length;
    int num_zeros = 0;
    int i = 0;

    for (; i < length; ++i) {
        if (*(arr->bytes + i) == 0)
            ++num_zeros;
    }

    return enif_make_int(env, num_zeros);
}


/** Encode the header and the following array as a binary for serialization */
static ERL_NIF_TERM
encode_registers(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    CARRAY_ENIF_GET(argv[0], arr);

    // We'll be needing space for both the header and the array.
    size_t num_bytes = sizeof(struct hyper_carray) + arr->length;

    // Copy the full segment into the binary term.
    ERL_NIF_TERM bin;
    unsigned char* data = enif_make_new_binary(env, num_bytes, &bin);
    memcpy(data, arr, num_bytes);

    return bin;
}


/** Given an encoded register, decode into our representation */
static ERL_NIF_TERM
decode_registers(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int precision = 0;
    ErlNifBinary bin;
    if (!enif_get_int(env, argv[1], &precision) ||
        !enif_inspect_binary(env, argv[0], &bin))
        return enif_make_badarg(env);

    // Create new carray_alloc with the specified precision.
    struct hyper_carray* arr = NULL;
    carray_alloc(precision, &arr);
    // Copy the entire segment.
    memcpy(arr, bin.data, sizeof(struct hyper_carray) + arr->length);

    ERL_NIF_TERM erl_res = enif_make_resource(env, arr);
    enif_release_resource(arr);
    return erl_res;
}



/** Called both for existing instances as well as new instances not yet created
    by the calling NIF library. */
void
dtor(ErlNifEnv* env, void* obj)
{
    // Just free the memory allocated for the pointer.
    enif_release_resource(obj);
}


/** Called when the NIF library is loaded and there is no previously loaded
    library for this module. */
static int
load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    // Create or open our resource type.
    ErlNifResourceFlags flags = (ErlNifResourceFlags)
        (ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    // Create or takeover our resource type and give it the destructor
    // function pointed to by dtor.
    CARRAY_RESOURCE =
        enif_open_resource_type(env, NULL, "hyper_carray", &dtor, flags, 0);

    return 0;
}


/** Called when the NIF library is loaded and there is already a previously
    loaded library for this module code. */
static int
reload(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    // Works the same as load. The only difference is that *priv_data already
    // contains the value set by the previous call to load or reload.
    return 0;
}


/** Called when the NIF library is loaded and there is old code of this module
    with a loaded NIF library. */
static int
upgrade(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM load_info)
{
    // Works the same as load. The only difference is that *old_priv_data
    // already contains the value set by the last call to load or reload for
    // the old module code. *priv_data will be initialized to NULL when upgrade
    // is called. It is allowed to write to both *priv_data and *old_priv_data.
    *priv = *old_priv;
    return 0;
}



/// Array of function descriptors for all the implemented NIFs in this library.
static ErlNifFunc hyper_funs[] =
{
    {"new",              1, new_register},
    {"set",              3, set},
    {"max_merge",        1, max_merge},
    {"bytes",            1, bytes},
    {"register_sum",     1, register_sum},
    {"zero_count",       1, zero_count},
    {"encode_registers", 1, encode_registers},
    {"decode_registers", 2, decode_registers}
};


// Initialize the NIF library.
ERL_NIF_INIT(hyper_carray, hyper_funs, &load, &reload, &upgrade, NULL);
