# Copyright (c) 2025 Pano Operations Ltd
#
# Use of this software is governed by the Business Source License included
# in the LICENSE file and at panoptisDev.com/bsl11.
#
# Change Date: 2028-4-16
#
# On the date above, in accordance with the Business Source License, use of
# this software will be governed by the GNU Lesser General Public License v3.

.PHONY: all clean

all: carmen-cpp

# this target builds the C++ library required by Go
carmen-cpp: 
	@cd ./go/lib ; \
	./build_libcarmen.sh ;

clean:
	cd ./go ; \
	rm -f lib/libcarmen.so ; \
	cd ../cpp ; \
	bazel clean ; \