package memory

// Stack 顺序栈
type Stack struct {
	Stack  []*sortTreeNode
	Length int
	Base   int // 栈底索引
	Top    int //  栈顶索引
}

// 简化的栈，不存在栈满的情况

// InitStack 初始化栈
func InitStack(n int) Stack {
	stack := Stack{
		Stack:  make([]*sortTreeNode, n),
		Length: n,
	}
	return stack
}

// Push 入栈
func (stack *Stack) Push(value *sortTreeNode) {
	// 栈满
	if stack.Top == stack.Length {
		stack.Stack = append(stack.Stack, value)
		stack.Length++
	} else {
		stack.Stack[stack.Top] = value
	}
	stack.Top++
}

// Pop 出栈
func (stack *Stack) Pop() (*sortTreeNode, bool) {
	// 空栈
	if stack.Top == stack.Base {
		return nil, false
	}
	// 下退一个位置
	stack.Top--
	return stack.Stack[stack.Top], true
}
