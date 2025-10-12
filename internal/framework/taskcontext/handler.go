package taskcontext

import (
	stdContext "context"
	"runtime/debug"

	"github.com/google/uuid"
	"github.com/xxcheng123/cloudpan189-share/internal/pkgs/taskengine"
	"go.uber.org/zap"
)

type HandlerFunc func(ctx *Context) error

type messageProcessor struct {
	handlerFunc HandlerFunc
	logger      *zap.Logger
	processorId string
}

func (p *messageProcessor) Process(ctx stdContext.Context, message []byte) error {
	defer func() {
		// 捕获panic并记录日志
		if err := recover(); err != nil {
			stackInfo := string(debug.Stack())
			p.logger.Error("task processor panic recovery",
				zap.Any("panic", err),
				zap.String("stack", stackInfo),
				zap.String("processor_id", p.processorId),
			)
		}
	}()

	return p.handlerFunc(newContext(ctx, message, p.logger))
}

func (p *messageProcessor) ProcessorID() string {
	return p.processorId
}

func newMessageProcessor(handlerFunc HandlerFunc, logger *zap.Logger) *messageProcessor {
	return &messageProcessor{
		handlerFunc: handlerFunc,
		logger:      logger,
		processorId: uuid.NewString(),
	}
}

type HandlerFuncWrapper struct {
	logger *zap.Logger
}

func (h *HandlerFuncWrapper) Wrap(fn HandlerFunc) taskengine.MessageProcessor {
	return newMessageProcessor(fn, h.logger)
}

func NewHandlerFuncWrapper(logger *zap.Logger) *HandlerFuncWrapper {
	return &HandlerFuncWrapper{logger: logger}
}
