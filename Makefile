PROGRAM=kaproxy-server

CCCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

all: $(PROGRAM)

.PHONY: all

$(PROGRAM):
	@sh build.sh
	@echo ""
	@printf $(MAKECOLOR)"Hint: It's a good idea to run 'make test' ;)"$(ENDCOLOR)
	@echo ""

test:
	- sh test.sh

coverage:

lint:
	@rm -rf lint.log
	@printf $(CCCOLOR)"Checking format...\n"$(ENDCOLOR)
	@go list ./... | sed -e 's=github.com/meitu/kaproxy=.=' | xargs -n 1 gofmt -d -s 2>&1 | tee lint.log
	@printf $(CCCOLOR)"Checking vet...\n"$(ENDCOLOR)
	@go list ./... | sed -e 's=github.com/meitu/kaproxy=.=' | xargs -n 1 go vet 2>&1 | tee lint.log
	@[ ! -s lint.log ]
