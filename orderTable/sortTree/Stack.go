package sortTree

// Stack 顺序栈
type Stack struct {
	stack []*treeNode
	base  int // 栈底索引
	top   int //  栈顶索引
}

// 简化的栈，不存在栈满的情况

// InitStack 初始化栈
func InitStack(n int) Stack {
	stack := Stack{
		stack: make([]*treeNode, n),
	}
	return stack
}

// Push 入栈
func (stack *Stack) Push(value *treeNode) {
	// 栈满
	if stack.top == len(stack.stack) {
		stack.stack = append(stack.stack, value)
	} else {
		stack.stack[stack.top] = value
	}
	stack.top++
}

// Pop 出栈
func (stack *Stack) Pop() (*treeNode, bool) {
	// 空栈
	if stack.top == stack.base {
		return nil, false
	}
	// 下退一个位置
	stack.top--
	return stack.stack[stack.top], true
}
