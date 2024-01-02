.PHONY: resolve
resolve:
	@echo "Resolving dependencies..."
	@go mod tidy
	@go mod vendor

.PHONY: run
run:
	@echo "Running..."
	@go run cmd/main.go

.PHONY: test
test:
	@echo "Running tests..."
	@go test -v ./...