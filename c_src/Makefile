
PROJECT_NIF_NAME=erlcass_nif

include nif.mk

ifeq ($(UNAME_SYS), darwin)

	CXXFLAGS += -I/usr/local/opt/openssl/include \
				-I/usr/local/include \

	LDFLAGS +=  -L/usr/local/opt/openssl/lib \
				-L/usr/local/lib \
				-Wl,-U,_enif_alloc \
				-Wl,-U,_enif_free \
				-Wl,-U,_enif_free_env \
                -Wl,-U,_enif_alloc_env \
				-Wl,-U,_enif_alloc_resource \
				-Wl,-U,_enif_open_resource_type \
				-Wl,-U,_enif_release_resource \
				-Wl,-U,_enif_priv_data \
				-Wl,-U,_enif_send \
				-Wl,-U,_enif_inspect_binary \
				-Wl,-U,_enif_inspect_iolist_as_binary \
				-Wl,-U,_enif_is_binary \
				-Wl,-U,_enif_is_identical \
				-Wl,-U,_enif_is_tuple \
				-Wl,-U,_enif_is_atom \
				-Wl,-U,_enif_is_list \
				-Wl,-U,_enif_get_atom \
				-Wl,-U,_enif_get_int \
				-Wl,-U,_enif_get_list_cell \
				-Wl,-U,_enif_get_resource \
				-Wl,-U,_enif_get_tuple \
				-Wl,-U,_enif_get_long \
				-Wl,-U,_enif_get_uint \
				-Wl,-U,_enif_get_double \
				-Wl,-U,_enif_get_list_length \
				-Wl,-U,_enif_get_ulong \
				-Wl,-U,_enif_get_local_pid \
				-Wl,-U,_enif_make_atom \
				-Wl,-U,_enif_make_existing_atom \
				-Wl,-U,_enif_make_new_binary \
				-Wl,-U,_enif_make_resource \
				-Wl,-U,_enif_make_tuple \
				-Wl,-U,_enif_make_list \
				-Wl,-U,_enif_make_int \
				-Wl,-U,_enif_make_list_from_array \
				-Wl,-U,_enif_make_badarg \
				-Wl,-U,_enif_make_copy \
				-Wl,-U,_enif_make_double \
				-Wl,-U,_enif_make_long \
				-Wl,-U,_enif_make_ref \
				-Wl,-U,_enif_make_tuple_from_array \
				-Wl,-U,_enif_make_uint \
				-Wl,-U,_enif_make_ulong
else
	# Needed for gcc-8
	CXXFLAGS+= -Wno-class-memaccess
endif

CXXFLAGS+= -O3 -std=c++11 -Wextra -Werror -fno-rtti \
			-Wno-unused-local-typedefs -Wno-unused-parameter -Wno-missing-field-initializers \
            -I $(BASEDIR)/_build/deps/cpp-driver/include \
            -I $(BASEDIR)/_build/deps/cpp-driver/src \
            -I $(BASEDIR)/_build/deps/cpp-driver/src/third_party/sparsehash/src \

LDFLAGS +=  -L$(BASEDIR)/_build/deps/cpp-driver/build \
            -lcassandra_static \
            -luv \
            -lstdc++ \
            -lssl \
            -lcrypto
