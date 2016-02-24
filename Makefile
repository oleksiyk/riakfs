ISTANBUL = ./node_modules/.bin/istanbul
ESLINT = ./node_modules/.bin/eslint
MOCHA = ./node_modules/.bin/mocha

ifeq ($(RIAK_ADMIN_USE_SUDO),true)
	RIAK_ADMIN = sudo riak-admin
else
	RIAK_ADMIN := riak-admin
endif

all: lint test coverage

# Tests
test: dt-setup
	@$(ISTANBUL) cover --report lcov --report text --report html _mocha

# Check code style
lint:
	@$(ESLINT) 'lib/**/*.js' 'test/**/*.js'

# Check coverage levels
coverage:
	@$(ISTANBUL) check-coverage --statement 85 --branch 70 --function 85

dt-setup:
	@$(RIAK_ADMIN) bucket-type create riakfs_stats '{"props":{"datatype":"map","allow_mult":true}}' > /dev/null || true
	@$(RIAK_ADMIN) bucket-type activate riakfs_stats > /dev/null || true

# Clean up
clean: clean-cov

clean-cov:
	@rm -rf coverage

.PHONY: all test lint coverage clean clean-cov dt-setup

