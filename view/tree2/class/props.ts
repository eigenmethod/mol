namespace $ {
	const err = $mol_view_tree2_error_str

	export function $mol_view_tree2_class_props(
		this: $,
		klass : $mol_tree2,
	) {
		const props = this.$mol_view_tree2_class_super( klass )

		const props_inner = [] as $mol_tree2[]

		const props_root = props.hack({

			'<=': ( operator, belt )=> {
				
				const prop = this.$mol_view_tree2_child( operator )
				
				const defs = prop.hack( belt )
				if( defs.length ) props_inner.push( prop.clone( defs ) )
				
				return [ operator.clone([ prop.clone([]) ]) ]
			},
			
			'<=>': ( operator, belt )=> {
				
				const prop = this.$mol_view_tree2_child( operator )
				
				const defs = prop.hack( belt )
				if( defs.length ) props_inner.push( prop.clone( defs ) )
				
				return [ operator.clone([ prop.clone([]) ]) ]
			},

		})

		return [ ... props_root , ... props_inner ]
	}

}
