package taskengine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xxcheng123/cloudpan189-share/internal/pkgs/ptr"

	"github.com/pkg/errors"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type TaskEngine interface {
	Start() error
	Stop() error
	IsRunning() bool

	RegisterProcessor(topic Topic, processor MessageProcessor) error
	PushMessage(ctx context.Context, topic Topic, payload []byte) error

	GetStats() TaskStats
	GetRunningTasks() []*TaskInfo
	GetPendingTasks() []*TaskInfo
}

type taskEngine struct {
	topicProcessors map[Topic][]MessageProcessor
	mu              sync.RWMutex
	running         bool
	options         *Options
	logger          *zap.Logger

	taskChan    chan *TaskInfo
	topCtx      context.Context
	topCancel   context.CancelFunc
	workerGroup sync.WaitGroup

	stats        *TaskStats
	runningTasks map[string]*TaskInfo
	pendingTasks map[string]*TaskInfo
	tasksMu      sync.RWMutex
}

type EngineOption struct {
	Logger  *zap.Logger
	Options []OptionFunc
}

func WithLogger(logger *zap.Logger) EngineOption {
	return EngineOption{
		Logger: logger,
	}
}

func NewTaskEngine(opts ...EngineOption) TaskEngine {
	logger := zap.NewNop()
	options := defaultOptions()

	for _, opt := range opts {
		if opt.Logger != nil {
			logger = opt.Logger
		}

		for _, optFunc := range opt.Options {
			optFunc(options)
		}
	}

	return &taskEngine{
		topicProcessors: make(map[Topic][]MessageProcessor),
		options:         options,
		logger:          logger,
		stats: &TaskStats{
			mu: &sync.RWMutex{},
		},
		runningTasks: make(map[string]*TaskInfo),
		pendingTasks: make(map[string]*TaskInfo),
	}
}

func (t *taskEngine) Start() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.running {
		return ErrEngineAlreadyRunning
	}

	t.running = true
	t.taskChan = make(chan *TaskInfo, t.options.BufferSize)
	t.topCtx, t.topCancel = context.WithCancel(context.Background())

	for idx := 0; idx < t.options.WorkerCount; idx++ {
		t.workerGroup.Add(1)

		go t.worker(fmt.Sprintf("worker_%d", idx))
	}

	t.logger.Info("task engine started",
		zap.Int("worker_count", t.options.WorkerCount),
		zap.Int("buffer_size", t.options.BufferSize))

	return nil
}

func (t *taskEngine) Stop() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.running {
		return ErrEngineNotRunning
	}

	t.logger.Info("stopping task engine...")

	// 停止接收新消息
	t.topCancel()

	// 关闭消息通道
	close(t.taskChan)
	// 等待所有工作协程结束
	t.workerGroup.Wait()

	t.running = false

	t.logger.Info("task engine stopped")

	return nil
}

func (t *taskEngine) IsRunning() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.running
}

func (t *taskEngine) worker(workerId string) {
	defer t.workerGroup.Done()

	t.logger.Debug("worker started", zap.String("worker_id", workerId))

	for {
		select {
		case <-t.topCtx.Done():
			t.logger.Debug("worker stopped", zap.String("worker_id", workerId))

			return
		case taskInfo, ok := <-t.taskChan:
			if !ok {
				t.logger.Debug("worker stopped due to channel closed", zap.String("worker_id", workerId))

				return
			}

			t.processMessage(taskInfo, workerId)
		}
	}
}

func (t *taskEngine) processMessage(taskInfo *TaskInfo, workerId string) {
	// 从待处理任务中删除
	t.tasksMu.Lock()
	delete(t.pendingTasks, taskInfo.ID)
	t.tasksMu.Unlock()

	// 获取处理器
	t.mu.RLock()
	processors, ok := t.topicProcessors[taskInfo.Topic]
	t.mu.RUnlock()

	if !ok {
		t.logger.Warn("no processor found for topic", zap.String("topic", string(taskInfo.Topic)))

		return
	}

	// 创建任务上下文
	taskCtx := t.newTaskContext(taskInfo, workerId)

	go func() {
		select {
		case <-t.topCtx.Done():
			t.logger.Debug("worker stopped due to engine stop", zap.String("task_id", taskCtx.taskId))

			taskCtx.Cancel()
		case <-taskCtx.ctx.Done():
			// 任务自然完成或被取消，无需额外操作
			t.logger.Debug("task completed or cancelled", zap.String("task_id", taskCtx.taskId))
		}
	}()

	// 记录运行中的任务
	t.addRunningTask(taskCtx.taskInfo)
	defer t.removeRunningTask(taskCtx.taskId)

	// 更新统计
	if t.options.EnableStats {
		t.stats.IncrementRunning()

		t.stats.DecrementPending()
		defer t.stats.DecrementRunning()
	}

	// 设置任务状态为运行中
	taskCtx.taskInfo.SetStatus(TaskStatusRunning)

	// 并发执行所有处理器
	var (
		wg       sync.WaitGroup
		hasError bool
		mu       sync.Mutex
	)

	for _, processor := range processors {
		wg.Add(1)

		go func(processor MessageProcessor) {
			defer wg.Done()

			startTime := time.Now()
			result := ProcessorResult{
				ProcessorID: processor.ProcessorID(),
				StartTime:   startTime,
				Status:      TaskStatusCompleted,
			}

			// 执行处理器
			if err := processor.Process(taskCtx.ctx, taskCtx.taskInfo.Payload); err != nil {
				mu.Lock()

				hasError = true

				mu.Unlock()

				result.Status = TaskStatusFailed
				result.Error = err.Error()

				if errors.Is(taskCtx.ctx.Err(), context.Canceled) {
					result.Status = TaskStatusCancelled
				}

				t.logger.Error("processor failed",
					zap.String("task_id", taskCtx.taskId),
					zap.String("processor_id", processor.ProcessorID()),
					zap.Error(err))
			}

			result.EndTime = time.Now()
			result.Duration = result.EndTime.Sub(result.StartTime)

			// 添加处理结果
			taskCtx.taskInfo.AddResult(result)
		}(processor)
	}

	// 等待所有处理器完成
	wg.Wait()

	// 在取消上下文之前先判断状态，避免主动取消导致的误判
	wasCancelled := errors.Is(taskCtx.ctx.Err(), context.Canceled)

	// 确保取消上下文
	taskCtx.Cancel()

	// 根据执行结果设置最终状态
	if wasCancelled {
		taskCtx.taskInfo.SetStatus(TaskStatusCancelled)
	} else if hasError {
		taskCtx.taskInfo.SetStatus(TaskStatusFailed)

		if t.options.EnableStats {
			t.stats.IncrementFailed()
		}
	} else {
		taskCtx.taskInfo.SetStatus(TaskStatusCompleted)

		if t.options.EnableStats {
			t.stats.IncrementCompleted()
		}
	}

	t.logger.Debug("task completed",
		zap.String("task_id", taskCtx.taskId),
		zap.String("status", taskCtx.taskInfo.GetStatus()),
		zap.Duration("duration", time.Since(taskCtx.taskInfo.ReceiveAt)))
}

func (t *taskEngine) newTaskContext(taskInfo *TaskInfo, workerId string) *TaskContext {
	// 应用超时控制
	ctx, cancel := context.WithTimeout(context.WithoutCancel(taskInfo.Context), t.options.ProcessTimeout)

	taskInfo.WorkerId = workerId
	taskInfo.StartAt = ptr.Of(time.Now())

	return &TaskContext{
		ctx:      ctx,
		cancel:   cancel,
		taskId:   taskInfo.ID,
		taskInfo: taskInfo,
	}
}

func (t *taskEngine) addRunningTask(taskInfo *TaskInfo) {
	t.tasksMu.Lock()
	defer t.tasksMu.Unlock()

	t.runningTasks[taskInfo.ID] = taskInfo
}

func (t *taskEngine) removeRunningTask(taskId string) {
	t.tasksMu.Lock()

	defer t.tasksMu.Unlock()

	delete(t.runningTasks, taskId)
}

func (t *taskEngine) RegisterProcessor(topic Topic, processor MessageProcessor) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.topicProcessors[topic] = append(t.topicProcessors[topic], processor)

	t.logger.Info("processor registered",
		zap.String("topic", string(topic)),
		zap.String("processor_id", processor.ProcessorID()))

	return nil
}

// PushMessage 推送消息，支持可选的最大重试次数参数
// 用法：
//
//	PushMessage(ctx, topic, payload)           // 使用默认重试次数
//	PushMessage(ctx, topic, payload, 5)        // 使用指定重试次数
func (t *taskEngine) PushMessage(ctx context.Context, topic Topic, payload []byte) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if !t.running {
		return ErrEngineNotRunning
	}

	taskInfo := &TaskInfo{
		Context:   ctx,
		ID:        uuid.New().String(),
		Payload:   payload,
		Topic:     topic,
		ReceiveAt: time.Now(),
		Status:    TaskStatusPending,
		Results:   make([]ProcessorResult, 0),
	}

	select {
	case t.taskChan <- taskInfo:
		t.tasksMu.Lock()
		t.pendingTasks[taskInfo.ID] = taskInfo
		t.tasksMu.Unlock()

		if t.options.EnableStats {
			t.stats.IncrementTotal()
			t.stats.IncrementPending()
		}

		return nil
	default:
		return ErrBufferFull
	}
}

func (t *taskEngine) GetStats() TaskStats {
	if !t.options.EnableStats {
		return TaskStats{}
	}

	return t.stats.GetStats()
}

func (t *taskEngine) GetRunningTasks() []*TaskInfo {
	t.tasksMu.RLock()
	defer t.tasksMu.RUnlock()

	tasks := make([]*TaskInfo, 0, len(t.runningTasks))
	for _, task := range t.runningTasks {
		tasks = append(tasks, task)
	}

	return tasks
}

func (t *taskEngine) GetPendingTasks() []*TaskInfo {
	t.tasksMu.RLock()
	defer t.tasksMu.RUnlock()

	tasks := make([]*TaskInfo, 0, len(t.pendingTasks))
	for _, task := range t.pendingTasks {
		tasks = append(tasks, task)
	}

	return tasks
}
