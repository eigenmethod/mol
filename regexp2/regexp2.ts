interface String {
	
	match< REMA extends RegExpMatchArray >(
		matcher: {
			[Symbol.match](string: string): REMA | null
		}
	): REMA | null
	
    matchAll< RE extends RegExp >(regexp: RE): IterableIterator<
		NonNullable<
			ReturnType<
				RE['exec']
			>
		>
	>
	
}

namespace $ {

	export type $mol_regexp2_source =
	| string
	| RegExp
	| $mol_regexp2_strict<{}>
	| { [ key in string ] : $mol_regexp2_source }
	| readonly[ $mol_regexp2_source , ... $mol_regexp2_source[] ]
	
	export type $mol_regexp2_strict<
		Groups extends Record< string, string >
	> = $mol_type_merge< RegExp & {
		[ Symbol.match ]( string: string ): $mol_type_override< RegExpMatchArray, { groups: Groups } > | null
		exec( string: string ): $mol_type_override< RegExpExecArray, { groups: Groups } > | null
	} >
	
	export type $mol_regexp2_groups< Source extends $mol_regexp2_source >
	
		= Source extends string
		? {}
		
		: Source extends $mol_regexp2_source[]
		? {
			[ key in Extract< keyof Source , number > ] : $mol_regexp2_groups< Source[ key ] >
		}[ Extract< keyof Source , number > ]
		
		: Source extends RegExp
		? NonNullable< NonNullable< ReturnType< Source['exec'] > >[ 'groups' ] >
		
		: Source extends { readonly [ key in string ] : $mol_regexp2_source }
		? {
			[ key in keyof Source ] :
				$mol_type_merge<
					& $mol_type_override<
						Record<
							Extract< keyof Source , string >,
							undefined 
						>,
						{
							[ k in key ]:
								Source[ key ] extends string
									? Source[ key ]
									: string
						}
					>
					& $mol_regexp2_groups< Source[ key ] >
				>
		}[ keyof Source ]

		: never

	/** Converts some js values to regexp */
	export function $mol_regexp2<
		Source extends $mol_regexp2_source
	>(
		source : Source ,
		{ ignoreCase , multiline } : Partial< Pick< RegExp , 'ignoreCase' | 'multiline' > > = {
			ignoreCase : false ,
			multiline : false ,
		} ,
	): $mol_regexp2_strict< $mol_regexp2_groups< Source > > {

		let flags = 'gsu'
		if( multiline ) flags += 'm'
		if( ignoreCase ) flags += 'i'

		if( typeof source === 'string' ) {

			const regexp = new RegExp( source.replace( /[.*+?^${}()|[\]\\]/g , '\\$&' ) , flags )
			return regexp as $mol_regexp2_strict< $mol_regexp2_groups< Source > >

		} else if( source instanceof RegExp ) {

			return source as any

		} if( Array.isArray( source ) ) {

			const chunks = source.map( src => $mol_regexp2( src ).source )
			const regexp = new RegExp( chunks.join( '' ) , flags ) as any
			return regexp as $mol_regexp2_strict< $mol_regexp2_groups< Source > >
	
		} else {

			const chunks = Object.keys( source ).map( name =>
				`(?<${ name }>${ $mol_regexp2( source[ name ] ).source })`
			)
			
			const regexp = new RegExp( `(?:${ chunks.join('|') })` , flags )
			return regexp as $mol_regexp2_strict< $mol_regexp2_groups< Source > >

		}

	}

	/** Makes regexp that non-greedy repeats this pattern from min to max count */
	export function $mol_regexp2_repeat<
		Source extends $mol_regexp2_source
	>(
		source : Source ,
		min = 0 ,
		max = Number.POSITIVE_INFINITY ,
	) {

		const regexp = $mol_regexp2( source )
		const upper = Number.isFinite( max ) ? max : ''
		
		const regexp2 = new RegExp( `(?:${ regexp.source }){${ min },${ upper }}?`, regexp.flags )
		return regexp2 as $mol_regexp2_strict< $mol_regexp2_groups< Source > >
	}

	/** Makes regexp that greedy repeats this pattern from min to max count */
	export function $mol_regexp2_repeat_greedy<
		Source extends $mol_regexp2_source
	>(
		source : Source ,
		min = 0 ,
		max = Number.POSITIVE_INFINITY ,
	) {

		const regexp = $mol_regexp2( source )
		const upper = Number.isFinite( max ) ? max : ''
		
		const regexp2 = new RegExp( `(?:${ regexp.source }){${ min },${ upper }}`, regexp.flags )
		return regexp2 as $mol_regexp2_strict< $mol_regexp2_groups< Source > >
	}

	/** Makes regexp that allow absent of this pattern */
	export function $mol_regexp2_optional<
		Source extends $mol_regexp2_source
	>( source : Source ) {
		return $mol_regexp2_repeat_greedy( source , 0 , 1 )
	}

	/** Makes regexp that look ahead for pattern */
	export function $mol_regexp2_force_after<
		Source extends $mol_regexp2_source
	>( source : Source ) {
		const regexp = $mol_regexp2( source )
		const regexp2 = new RegExp( `(?=${ regexp.source })`, regexp.flags )
		return regexp2 as $mol_regexp2_strict< $mol_regexp2_groups< Source > >
	}

	/** Makes regexp that look ahead for pattern */
	export function $mol_regexp2_forbid_after<
		Source extends $mol_regexp2_source
	>( source : Source ) {
		const regexp = $mol_regexp2( source )
		const regexp2 = new RegExp( `(?!${ regexp.source })`, regexp.flags )
		return regexp2 as $mol_regexp2_strict< $mol_regexp2_groups< Source > >
	}

	/** Makes regexp for char code */
	export function $mol_regexp2_char_code( code : number ) {
		return new RegExp( `\\u{${ code.toString(16).padStart( 4 , '0' ) }}`, 'gsu' )
	}

	/** Makes regexp which includes only unicode category */
	export function $mol_regexp2_unicode_only( ... category: $mol_unicode_category ) {
		return new RegExp( `\\p{${ category.join( '=' ) }}`, 'gsu' )
	}

	/** Makes regexp which excludes unicode category */
	export function $mol_regexp2_unicode_except( ... category: $mol_unicode_category ) {
		return new RegExp( `\\P{${ category.join( '=' ) }}`, 'gsu' )
	}

	/** Makes regexp part for character range */
	export function $mol_regexp2_char_range(
		from: number,
		to: number,
	) {
		return new RegExp(
			`${ $mol_regexp2_char_code( from ) }..${ $mol_regexp2_char_code( to ) }`,
			'gsu'
		)
	}

	/** Makes regexp which includes only characters */
	export function $mol_regexp2_char_only(
		... allowed: readonly [ $mol_regexp2_source, ... $mol_regexp2_source[] ]
	) {
		return new RegExp( `[${ $mol_regexp2( allowed ).source }]`, 'gsu' )
	}

	/** Makes regexp which excludes characters */
	export function $mol_regexp2_char_except(
		... forbidden: readonly [ $mol_regexp2_source, ... $mol_regexp2_source[] ]
	) {
		return new RegExp( `[^${ $mol_regexp2( forbidden ).source }]`, 'gsu' )
	}

	export const $mol_regexp2_char_any = $mol_regexp2( /./ )
	export const $mol_regexp2_line_end = $mol_regexp2( /(?:\r?\n|\r)/ )
	
	export const $mol_regexp2_decimal_only = $mol_regexp2( /\d/ )
	export const $mol_regexp2_decimal_except = $mol_regexp2( /\D/ )
	
	export const $mol_regexp2_latin_only = $mol_regexp2( /\w/ )
	export const $mol_regexp2_latin_except = $mol_regexp2( /\W/ )
	
	export const $mol_regexp2_space_only = $mol_regexp2( /\s/ )
	export const $mol_regexp2_space_except = $mol_regexp2( /\S/ )
	
	export const $mol_regexp2_word_break_only = $mol_regexp2( /\b/ )
	export const $mol_regexp2_word_break_except = $mol_regexp2( /\B/ )
	
	export const $mol_regexp2_tab = $mol_regexp2( /\t/ )
	export const $mol_regexp2_slash_back = $mol_regexp2( /\\/ )
	export const $mol_regexp2_nul = $mol_regexp2( /\0/ )
	
	export const $mol_regexp2_begin = $mol_regexp2( /^/ )
	export const $mol_regexp2_end = $mol_regexp2( /$/ )
	
}
