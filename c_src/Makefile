
PROJECT_NIF_NAME=erlcass_nif

include nif.mk

ifeq ($(UNAME_SYS), Darwin)
	CXXFLAGS += -I/usr/local/opt/openssl/include \
				-I/usr/local/include/
	LDFLAGS +=  -L/usr/local/opt/openssl/lib \
				-L/usr/local/lib/
else ifeq ($(UNAME_SYS), Linux)
	CXXFLAGS += -Wno-unused-local-typedefs
endif

CXXFLAGS+=  -g -std=c++11 -Wextra -Werror -Wno-unused-parameter -Wno-missing-field-initializers -fno-rtti \
            -isystem $(BASEDIR)/_build/deps/cpp-driver/include \
            -isystem $(BASEDIR)/_build/deps/cpp-driver/src \
            -I $(BASEDIR)/_build/deps/cpp-driver/src/third_party/sparsehash/src \

LDFLAGS +=  -L$(BASEDIR)/_build/deps/cpp-driver/build \
            -lcassandra_static \
            -luv \
            -lstdc++ \
            -lssl \
            -lcrypto
