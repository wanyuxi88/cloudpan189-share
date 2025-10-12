package httpcontext

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"time"

	"github.com/xxcheng123/cloudpan189-share/internal/framework/context"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type HandlerFunc func(ctx *Context)

type HandlerFuncWrapper struct {
	logger *zap.Logger
}

func NewHandlerFuncWrapper(logger *zap.Logger) *HandlerFuncWrapper {
	return &HandlerFuncWrapper{logger: logger}
}

const httpContextKey = "__request__context__"

func (w *HandlerFuncWrapper) Wrap(handler HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		if v, ok := c.Get(httpContextKey); ok {
			if ctx, ok2 := v.(*Context); ok2 {
				handler(ctx)

				return
			}
		}

		ctx := newContext(c, w.logger)

		c.Set(httpContextKey, ctx)

		handler(ctx)
	}
}

func (w *HandlerFuncWrapper) Wraps(handlers ...HandlerFunc) []gin.HandlerFunc {
	hds := make([]gin.HandlerFunc, 0, len(handlers))
	for _, h := range handlers {
		hds = append(hds, w.Wrap(h))
	}

	return hds
}

func loggerHandler() HandlerFunc {
	return func(reqContext *Context) {
		var (
			ts      = time.Now()
			ctx     = reqContext.GetContext()
			traceId = ctx.ID()

			fields = make([]zap.Field, 0)
		)

		reqContext.Writer.Header().Set(traceHeaderKey, traceId)

		var (
			decodedURL, _ = url.QueryUnescape(reqContext.Request.URL.RequestURI())
			requestInfo   = &context.Request{
				TTL:        "un-limit",
				Method:     reqContext.Request.Method,
				DecodedURL: decodedURL,
				Header:     reqContext.Request.Header.Clone(),
			}
		)

		// 只解析 json
		if strings.Contains(reqContext.Request.Header.Get("Content-Type"), "application/json") {
			if body, err := reqContext.GetRawData(); err == nil && len(body) > 0 {
				reqContext.Request.GetBody = func() (io.ReadCloser, error) {
					reqContext.Request.Body = io.NopCloser(bytes.NewBuffer(body))
					buffer := bytes.NewBuffer(body)
					closer := io.NopCloser(buffer)

					return closer, nil
				}

				bb, _ := reqContext.Request.GetBody()
				reqContext.Request.Body = bb

				requestInfo.Body = string(body)
			}
		}

		writer := &responseWriter{
			ResponseWriter: reqContext.Writer,
			b:              &bytes.Buffer{},
		}
		reqContext.Writer = writer

		defer func() {
			// region 发生 Panic 异常发送告警提醒
			if err := recover(); err != nil {
				stackInfo := string(debug.Stack())
				fields = append(fields,
					zap.Any("panic", err),
					zap.String("stack", stackInfo),
					zap.String("trace_id", traceId),
				)
				reqContext.GetContext().Error("HTTP panic recovery", fields...)
				_ = reqContext.AbortWithError(http.StatusInternalServerError, errors.New("内部服务器错误"))
			}

			cost := time.Since(ts).Seconds()
			success := reqContext.Writer.Status() == http.StatusOK

			respInfo := &context.Response{
				Header:      reqContext.Writer.Header(),
				HttpCode:    reqContext.Writer.Status(),
				HttpCodeMsg: http.StatusText(reqContext.Writer.Status()),
				CostSeconds: cost,
			}
			if strings.Contains(reqContext.Writer.Header().Get("Content-Type"), "application/json") {
				respInfo.Body = writer.b.String()
			}

			ctx.WithRequest(requestInfo)
			ctx.WithResponse(respInfo)

			fields = append(fields,
				zap.String("method", reqContext.Request.Method),
				zap.String("path", decodedURL),
				zap.Int("http_code", reqContext.Writer.Status()),
				zap.Bool("success", success),
				zap.Float64("cost_seconds", cost),
				zap.Any("trace_info", ctx.Trace),
				zap.Errors("errors", reqContext.errors),
				zap.String("client_ip", reqContext.ClientIP()),
			)

			ctx.Info("http-log", fields...)
		}()

		reqContext.Next()
	}
}

func LoggerHandler(logger *zap.Logger) gin.HandlerFunc {
	return NewHandlerFuncWrapper(logger).Wrap(loggerHandler())
}

type responseWriter struct {
	gin.ResponseWriter
	b *bytes.Buffer
}

func (w responseWriter) Write(data []byte) (int, error) {
	// 如果还没超限，就记录日志
	if w.b.Len() <= 1024*1024 {
		w.b.Write(data)
	}

	// 正常写响应
	return w.ResponseWriter.Write(data)
}
