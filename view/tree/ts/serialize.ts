namespace $ {
	export function $mol_view_tree_ts_serialize( node: $mol_tree, prefix = '') : string {
		if (node.type === 'line') return prefix + node.sub.map(child => $mol_view_tree_ts_serialize(child)).join(' ')
		if (node.type === 'inline') return node.sub.map(child => $mol_view_tree_ts_serialize(child)).join('')
		const is_root = ! node.data && ! node.type
		const child_prefix = is_root ? prefix : (prefix + '\t')
		const childs = node.sub.map(child => $mol_view_tree_ts_serialize(child, child_prefix)).join('\n')

		if (is_root) return childs + '\n'
		if (node.type === 'block') return '\n' + childs + '\n'

		return prefix + (node.data || node.type) + childs
	}
}
