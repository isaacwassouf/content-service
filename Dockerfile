FROM golang:1.22-alpine AS builder

# Set destination for COPY
WORKDIR /app

# Download Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code. Note the slash at the end, as explained in
# https://docs.docker.com/reference/dockerfile/#copy
COPY ./ ./

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /main

FROM alpine:latest

# Copy the binary from the builder stage
COPY --from=builder /main /main

# Expose the port
EXPOSE 8080

# Run the binary
CMD ["/main"]

