package memory

// Stack 顺序栈
type Stack struct {
	stack  []*sortTreeNode
	length int
	base   int // 栈底索引
	top    int //  栈顶索引
}

// 简化栈，不存在栈满的情况

// Push 入栈
func (stack *Stack) Push(value *sortTreeNode) {
	// 栈满
	if stack.top == stack.length {
		stack.stack = append(stack.stack, value)
		stack.length++
	} else {
		stack.stack[stack.top] = value
	}
	stack.top++
}

// Pop 出栈
func (stack *Stack) Pop() (*sortTreeNode, bool) {
	// 空栈
	if stack.top == stack.base {
		return nil, false
	}
	// 下退一个位置
	stack.top--
	return stack.stack[stack.top], true
}
