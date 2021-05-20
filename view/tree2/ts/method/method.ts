namespace $ {

	export function $mol_view_tree2_ts_method(
		this: $,
		owner_parts: $mol_view_tree2_prop,
		body: $mol_tree2,
		types = false
	) {
		const { name, key, next, src } = owner_parts
		const operator = src.kids.length === 1 ? src.kids[0] : undefined
		const type = operator?.type
		const is_class = type && type[0] === '$'
		const is_locale = type == '@'
		const is_delegate = type === '<=' || type === '<=>'

		let need_cache = false
		if (is_delegate) need_cache = false
		else if (next !== undefined) need_cache = true
		else if (is_class || is_locale) need_cache = true

		const sub: $mol_tree2[] = this.$mol_view_tree2_ts_comment_doc(src)

		if (need_cache && key) sub.push(name.data(`@ $${''}mol_mem_key`)) 
		if (need_cache && ! key) sub.push(name.data(`@ $${''}mol_mem`))

		sub.push(
			name.struct('line', [
				name,
				$mol_view_tree2_ts_function_declaration(owner_parts, types),
				name.data(' {'),
			])
		)

		if (next && need_cache) sub.push(
			next.struct('indent', [
				next.struct('line', [
					next.data('if ( '),
					next,
					next.data(' !== undefined ) return '),
					next,
				])
			])
		)

		sub.push(body, name.data('}'))

		return sub
	}
}
