BUILD_VERSION   := v1.0.0
BUILD_NAME      := kafka_exporter
TARGET_DIR      := ./release
COMMIT_SHA1     := $(shell git rev-parse HEAD || echo unsupported)

all:
        CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
        go build \
        -a -ldflags -extldflags=-static \
        -ldflags  "-X 'main.buildTime=`date "+%Y-%m-%d %H:%M:%S"`' -X 'main.goVersion=`go version`' -X main.buildName=${BUILD_NAME} -X main.commitID=${COMMIT_SHA1}" \
        -o ${BUILD_NAME}

clean:
        rm ${BUILD_NAME} -f

release:
        mkdir -p ${TARGET_DIR}
        cp ${BUILD_NAME} ${TARGET_DIR} -f

.PHONY : all clean release ${BUILD_NAME}