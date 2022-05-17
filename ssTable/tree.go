package ssTable

// 插入一个 SsTable 到指定层
func (tree *TableTree) insert(table *SsTable, level int) (index int) {
	tree.lock.Lock()
	defer tree.lock.Unlock()

	// 每次插入的，都出现在最后面
	node := tree.levels[level]
	newNode := &tableNode{
		table: table,
		next:  node,
		index: 0,
	}

	if node == nil {
		tree.levels[level] = newNode
	} else {
		for node != nil {
			if node.next == nil {
				newNode.index = node.index + 1
				node.next = newNode
			}
		}
	}
	return newNode.index
}
