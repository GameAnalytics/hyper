// Copyright (c) 2014 Johannes Huning <mail@johanneshuning.com>
// Copyright (c) 2015 Christian Lundgren, Chris de Vries, and Jon Elverkilde
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

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <tgmath.h>

#include "erl_nif.h"

/*
 * Erlang NIF resource type used to 'tag' hyper_carrays.
 */
static ErlNifResourceType *carray_resource;

/*
 * Single resource produced and consumed by these NIFs.
 * Header placed directly in front of the items it points to.
 */
struct hyper_carray {
	/*
	 * Precision = log_2(size). Handy to have it.
	 */
	unsigned int precision;
	/*
	 * Number of items.
	 */
	unsigned int size;
	/*
	 * Array of items each one byte in size.
	 */
	uint8_t *items;
};

typedef struct hyper_carray *restrict carray_ptr;

#define HYPER_CARRAY_SIZE sizeof(struct hyper_carray)

/*
 * Attempts to read a hyper_carray from _term into _varname.
 * Returns badarg on failure to do so.
 */
#define HYPER_CARRAY_OR_BADARG(_term, _varname) \
    void* _varname_res = NULL; \
    if (!enif_get_resource(env, _term, carray_resource, &_varname_res)) \
        return enif_make_badarg(env); \
    _varname = _varname_res;

/*
 * Allocate a new hyper_carray for use as an Erlang NIF resource.
 */
static void carray_alloc(unsigned int precision, carray_ptr * arr)
{
	unsigned int nitems = pow(2, precision);
	size_t header_size = HYPER_CARRAY_SIZE;
	size_t res_size = header_size + nitems;

	void *res = enif_alloc_resource(carray_resource, res_size);
	*arr = res;

	memset(*arr, 0, header_size);
	(*arr)->precision = precision;
	(*arr)->size = nitems;
	(*arr)->items = res + header_size;
}

/*
 * Given an hyper_carray and a valid index, set the value at that index to
 * max(current value, given value).
 */
static inline void carray_merge_item(carray_ptr arr,
				     unsigned int index,
				     unsigned int value)
{
	uint8_t *item = arr->items + index;
	*item = (value > *item) ? value : *item;
}

/*
 * Create a new hyper_carray resource with all items set to 0.
 */
static ERL_NIF_TERM new_hyper_carray(ErlNifEnv * env, int argc,
				     const ERL_NIF_TERM argv[])
{
	unsigned int precision = 0;
	if (!enif_get_uint(env, argv[0], &precision))
		return enif_make_badarg(env);

	carray_ptr arr = NULL;
	carray_alloc(precision, &arr);
	memset(arr->items, 0, arr->size);

	ERL_NIF_TERM erl_res = enif_make_resource(env, arr);
	enif_release_resource(arr);
	return erl_res;
}

/*
 * NIF variant of carray_merge_item (see above).
 */
static ERL_NIF_TERM set(ErlNifEnv * env, int argc,
			const ERL_NIF_TERM argv[])
{
	carray_ptr arr = NULL;
	HYPER_CARRAY_OR_BADARG(argv[2], arr);

	unsigned int index = 0;
	unsigned int new_value = 0;
	if (!enif_get_uint(env, argv[0], &index)
	    || !enif_get_uint(env, argv[1], &new_value))
		return enif_make_badarg(env);

	// Validate bounds
	if (index > arr->size - 1)
		return enif_make_badarg(env);

	carray_merge_item(arr, index, new_value);

	return argv[2];
}

void dtor(ErlNifEnv * env, void *obj);

/*
 * Given a list of at least 1 hyper_carrays [A,B,...], merge into a single new
 * hyper_carray N. Where the i-ths item N[i] is max(A[i], B[i], ...).
 * A, B, and so on are assumed to be _different_ hyper_carrays.
 */
static ERL_NIF_TERM max_merge(ErlNifEnv * env, int argc,
			      const ERL_NIF_TERM argv[])
{
	unsigned int narrays = 0;
	ERL_NIF_TERM head;
	ERL_NIF_TERM tail;

	if (!enif_get_list_length(env, argv[0], &narrays)
	    || !enif_get_list_cell(env, argv[0], &head, &tail))
		return enif_make_badarg(env);

	if (narrays < 1)
		return enif_make_badarg(env);

	carray_ptr first = NULL;
	HYPER_CARRAY_OR_BADARG(head, first);
	const unsigned int nitems = first->size;

	carray_ptr acc = NULL;
	carray_alloc(first->precision, &acc);
	memcpy(acc->items, first->items, acc->size);

	// Merge arrays
	for (int i = 1; i < narrays; ++i) {
		carray_ptr curr = NULL;

		if (!enif_get_list_cell(env, tail, &head, &tail)
		    || !enif_get_resource(env, head, carray_resource,
					  (void *) &curr))
			goto dealloc_and_badarg;

		// Require uniform precision.
		if (curr->precision != acc->precision)
			goto dealloc_and_badarg;

		for (uint8_t * accitem = acc->items, *item = curr->items,
		     *enditem = curr->items + nitems;
		     item != enditem; ++item, ++accitem) {
			*accitem = (*item > *accitem) ? *item : *accitem;
		}

		continue;

	      dealloc_and_badarg:
		dtor(env, acc);
		return enif_make_badarg(env);
	}

	ERL_NIF_TERM erl_res = enif_make_resource(env, acc);
	enif_release_resource(acc);
	return erl_res;
}

/*
 * Return the total number of bytes allocated for the given hyper_carray.
 * Includes the header's size.
 */
static ERL_NIF_TERM bytes(ErlNifEnv * env, int argc,
			  const ERL_NIF_TERM argv[])
{
	carray_ptr arr = NULL;
	HYPER_CARRAY_OR_BADARG(argv[0], arr);
	return enif_make_int(env, HYPER_CARRAY_SIZE + arr->size);
}

/*
 * Sum over 2^-X where X is the value of each item in the given hyper_carray.
 */
static ERL_NIF_TERM register_sum(ErlNifEnv * env, int argc,
				 const ERL_NIF_TERM argv[])
{
	carray_ptr arr = NULL;
	HYPER_CARRAY_OR_BADARG(argv[0], arr);

	int currval = 0;
	double sum = 0.0;
	unsigned int size = arr->size;

	for (int i = 0; i < size; ++i) {
		currval = arr->items[i];
		sum += pow(2, -currval);
	}

	return enif_make_double(env, sum);
}

/*
 * Number of items with a 0 as value;
 */
static ERL_NIF_TERM zero_count(ErlNifEnv * env, int argc,
			       const ERL_NIF_TERM argv[])
{
	carray_ptr arr = NULL;
	HYPER_CARRAY_OR_BADARG(argv[0], arr);

	unsigned int nzeros = 0;
	unsigned int size = arr->size;

	for (int i = 0; i < size; ++i) {
		if (arr->items[i] == 0)
			++nzeros;
	}

	return enif_make_int(env, nzeros);
}

/*
 * Encode the given hyper_carray as an Erlang binary.
 */
static ERL_NIF_TERM encode_registers(ErlNifEnv * env, int argc,
				     const ERL_NIF_TERM argv[])
{
	carray_ptr arr = NULL;
	HYPER_CARRAY_OR_BADARG(argv[0], arr);

	size_t nbytes = arr->size;

	ERL_NIF_TERM bin;
	unsigned char *buf = enif_make_new_binary(env, nbytes, &bin);
	memcpy(buf, arr->items, nbytes);

	return bin;
}

/*
 * Decode the given serialized hyper_carray into a new resource.
 */
static ERL_NIF_TERM decode_registers(ErlNifEnv * env, int argc,
				     const ERL_NIF_TERM argv[])
{
	unsigned int precision = 0;
	ErlNifBinary bin;

	if (!enif_get_uint(env, argv[1], &precision)
	    || !enif_inspect_binary(env, argv[0], &bin))
		return enif_make_badarg(env);

	carray_ptr arr = NULL;
	carray_alloc(precision, &arr);
	memcpy(arr->items, bin.data, arr->size);

	ERL_NIF_TERM erl_res = enif_make_resource(env, arr);
	enif_release_resource(arr);

	return erl_res;
}

/*
 * Map of funs to NIFs.
 */
static ErlNifFunc niftable[] = {
	{"new", 1, new_hyper_carray},
	{"set", 3, set},
	{"max_merge", 1, max_merge},
	{"bytes", 1, bytes},
	{"register_sum", 1, register_sum},
	{"zero_count", 1, zero_count},
	{"encode_registers", 1, encode_registers},
	{"decode_registers", 2, decode_registers}
};

/*
 * Destructor for hyper_carray resources.
 */
void dtor(ErlNifEnv * env, void *obj)
{
	enif_release_resource(obj);
}

/*
 * Creates or opens the hyper_carray resource _type_.
 * Registers dtor to be called on garbage collection of hyper_carrays.
 * Please see http://www.erlang.org/doc/man/erl_nif.html.
 */
static int load(ErlNifEnv * env, void **priv_data, ERL_NIF_TERM load_info)
{
	carray_resource =
	    enif_open_resource_type(env, NULL, "hyper_carray", &dtor,
				    ERL_NIF_RT_CREATE |
				    ERL_NIF_RT_TAKEOVER, 0);
	return 0;
}

/*
 * Called when the NIF library is loaded and there is old code of this module
 * with a loaded NIF library.
 */
static int upgrade(ErlNifEnv * env, void **priv, void **old_priv,
		   ERL_NIF_TERM load_info)
{
	*priv = *old_priv;
	return 0;
}

/*
 * Initialize the NIF library.
 */
ERL_NIF_INIT(hyper_carray, niftable, &load, NULL, &upgrade, NULL);
