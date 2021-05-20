namespace $ {
	export function $mol_view_tree2_ts_locale(operator: $mol_tree2, context: $mol_view_tree2_context) {
		return [ operator.struct('line', [
			operator.data('this.$.$mol_locale.t( {key: \''),
			context.locale(operator),
			operator.data('\', params: this})'),
		]) ]
	}
}
