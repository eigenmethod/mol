namespace $ {

	export function $mol_atom2_value< Value >( task : ()=> Value ) : Value | undefined {
		const cached = $mol_atom2.cached
		try {
			$mol_atom2.cached = true
			return task()
		} finally {
			$mol_atom2.cached = cached
		}
	}

	export class $mol_atom2< Value = any > extends $mol_fiber< Value > {

		static get current() {
			const atom = $mol_fiber.current
			if( atom instanceof $mol_atom2 ) return atom
			return null
		}

		static cached = false

		static reap_task = null as null | $mol_fiber
		static reap_queue = [] as $mol_atom2[]

		static reap( atom : $mol_atom2 ) {

			this.reap_queue.push( atom )

			if( this.reap_task ) return

			this.reap_task = $mol_fiber_defer( ()=> {
				this.reap_task = null
				
				while( true ) {
					
					const atom = this.reap_queue.pop()
					if( !atom ) break
					
					if( !atom.alone ) continue
					
					atom.destructor()
				}
				
			} )

		}

		slaves = [] as ( $mol_fiber | number | undefined )[]
		
		rescue( master : $mol_atom2 , cursor : number ) {

			if(!( master instanceof $mol_atom2 )) return
			
			const master_index = this.masters.length
			const slave_index = this.masters[ cursor + 1 ] as number + 1
			
			master.slaves[ slave_index ] = master_index
			this.masters.push( master , this.masters[ cursor + 1 ] )
			
		}

		get() {
			if( $mol_atom2.cached ) return this.value
			
			const value = super.get()
			if( value === undefined ) $mol_fail( new Error( `Not defined: ${ this }` ) )
			
			return value
		}

		pull() {
			
			if( this.cursor === $mol_fiber_status.obsolete ) return super.pull()

			this.$.$mol_log2.info( this , $mol_atom2_token_revalidation )
			
			const masters = this.masters

			for( let index = 0 ; index < masters.length ; index += 2 ) {

				const master = masters[ index ] as $mol_atom2
				if( !master ) continue

				try {
					master.get()
				} catch( error ) {
					if( 'then' in error ) $mol_fail_hidden( error )
					this.cursor = $mol_fiber_status.obsolete
				}

				if( this.cursor as $mol_fiber_status !== $mol_fiber_status.obsolete ) continue

				this.$.$mol_log2.info( this , $mol_atom2_token_stumbled , this._error || this._value )
				return super.pull()
			}

			this.$.$mol_log2.info( this , $mol_atom2_token_revalidated , this._error || this._value )
			this.cursor = $mol_fiber_status.actual

		}
		
		_value = undefined as unknown as Value
		get value() { return this._value }
		set value( next : Value ) {

			const prev = this._value

			if( prev && this.$.$mol_owning_check( this , prev ) ) prev.destructor()

			if( next && this.$.$mol_owning_catch( this , next ) ) {
				next[ Symbol.toStringTag ] = this[ Symbol.toStringTag ]
				next[ $mol_object_field ] = this[ $mol_object_field ]
			}

			this._value = next
		}

		_error = null as null | Error | PromiseLike< Value >
		get error() { return this._error }
		set error( next : null | Error | PromiseLike< Value > ) {

			const prev = this._error

			if( prev && this.$.$mol_owning_check( this , prev ) ) prev.destructor()

			if( next && this.$.$mol_owning_catch( this , next ) ) {
				next[ Symbol.toStringTag ] = this[ Symbol.toStringTag ]
				next[ $mol_object_field ] = this[ $mol_object_field ]
			}

			this._error = next
		}

		wait( promise : PromiseLike< Value > ) : PromiseLike< Value > {

			this.$.$mol_log2.info( this , $mol_fiber_token_sleeped , promise )
						
			// if( this.error === promise ) return promise

			this.error = promise
			this.cursor = $mol_fiber_status.actual

			this.obsolete_slaves()
			
			return promise

		}
		
		@ $mol_log2_indent.method
		update() {

			const slave = $mol_fiber.current
			
			try {
				
				$mol_fiber.current = this

				this.$.$mol_log2.info( this , $mol_fiber_token_runned )

				this.pull()

			} catch( error ) {

				if( 'then' in error ) {
					
					if( !error['atom-catched'] ) {
						error['atom-catched'] = true
						const listener = ()=> this.obsolete()
						error = error.then( listener , listener )
					}

					this.wait( error )

				}

				this.fail( error )
				
			} finally {
				$mol_fiber.current = slave
			}

		}

		put( next : Value ) {
			this.cursor = this.masters.length
			next = this.push( next )
			this.cursor = $mol_fiber_status.persist
			return next
		}

		complete_master( master_index : number ) {
			if( this.masters[ master_index ] instanceof $mol_atom2 ) {
				if( master_index >= this.cursor ) this.disobey( master_index )
			} else {
				this.disobey( master_index )
			}
		}

		obey( master : $mol_fiber , master_index : number ) : number {
			return master.lead( this , master_index )
		}

		lead( slave : $mol_fiber , master_index : number ) {
			
			this.$.$mol_log2.info( this , $mol_atom2_token_leaded , slave )
			
			const slave_index = this.slaves.length
			this.slaves[ slave_index ] = slave
			this.slaves[ slave_index + 1 ] = master_index
			
			return slave_index
		}

		dislead( slave_index : number ) {

			if( slave_index < 0 ) return // slave is fiber
			
			this.$.$mol_log2.info( this , $mol_atom2_token_disleaded , this.slaves[ slave_index ] )

			this.slaves[ slave_index ] = undefined
			this.slaves[ slave_index + 1 ] = undefined

			$mol_array_trim( this.slaves )

			if( this.cursor > $mol_fiber_status.persist && this.alone ) $mol_atom2.reap( this )
		}

		obsolete( master_index = -1 ) {

			if( this.cursor > $mol_fiber_status.obsolete ) {
				if( master_index >= this.cursor - 2 ) return

				const path = [] as $mol_atom2[]
				let current = this as $mol_atom2
				
				collect : while( current ) {
					path.push( current )
					current = current.masters[ current.cursor - 2 ] as $mol_atom2
				}

				this.$.$mol_fail( new Error( `Obsoleted while calculation \n\n${ path.join( '\n' ) }\n` ) )
			}
			
			if( this.cursor === $mol_fiber_status.obsolete ) return
			
			this.$.$mol_log2.info( this , $mol_atom2_token_obsoleted , this._error || this._value )
			if( this.cursor !== $mol_fiber_status.doubt ) this.doubt_slaves()
			
			this.cursor = $mol_fiber_status.obsolete
			
		}

		doubt( master_index = -1 ) {
			
			if( this.cursor > $mol_fiber_status.obsolete ) {
				if( master_index >= this.cursor - 2 ) return

				const path = [] as $mol_atom2[]
				let current = this as $mol_atom2
				
				collect : while( current ) {
					path.push( current )
					current = current.masters[ current.cursor - 2 ] as $mol_atom2
				}

				this.$.$mol_fail( new Error( `Doubted while calculation \n\n${ path.join( '\n' ) }\n` ) )
			}
			
			if( this.cursor >= $mol_fiber_status.doubt ) return
				
			this.$.$mol_log2.info( this , $mol_atom2_token_doubted , this._error || this._value )
			this.cursor = $mol_fiber_status.doubt
			
			this.doubt_slaves()

		}

		@ $mol_log2_indent.method
		obsolete_slaves() {
			for( let index = 0 ; index < this.slaves.length ; index += 2 ) {
				const slave = this.slaves[ index ] as $mol_atom2
				if( slave ) slave.obsolete( this.slaves[ index + 1 ] as number )
			}
		}

		@ $mol_log2_indent.method
		doubt_slaves() {
			for( let index = 0 ; index < this.slaves.length ; index += 2 ) {
				const slave = this.slaves[ index ] as $mol_atom2
				if( slave ) slave.doubt( this.slaves[ index + 1 ] as number )
			}
		}

		get fresh() {
			return $mol_log2_hidden.func( ()=> {
				if( this.cursor !== $mol_fiber_status.actual ) return

				this.cursor = $mol_fiber_status.obsolete
				$mol_fiber_solid.run( ()=> this.update() )
			} )
		}

		get alone() {
			return this.slaves.length === 0
		}
		
		get derived() {
			
			for( let index = 0 ; index < this.masters.length ; index += 2 ) {
				if( this.masters[ index ] ) return true
			}

			return false
		}

		destructor() {

			if( !this.abort() ) return
			
			this.$.$mol_log2.info( this , $mol_fiber_token_destructed )

			this.cursor = $mol_fiber_status.persist

			for( let index = 0 ; index < this.masters.length ; index += 2 ) {
				this.complete_master( index )
			}

		}
		
	}

	export let $mol_atom2_token_revalidation = new $mol_log2_token( ' ⏭ ' )
	export let $mol_atom2_token_stumbled = new $mol_log2_token( ' ⏯ ' )
	export let $mol_atom2_token_revalidated = new $mol_log2_token( ' ✔ ' )
	export let $mol_atom2_token_leaded = new $mol_log2_token( ' ☍ ' )
	export let $mol_atom2_token_disleaded = new $mol_log2_token( ' ☌ ' )
	export let $mol_atom2_token_obsoleted = new $mol_log2_token( ' ✘ ' )
	export let $mol_atom2_token_doubted = new $mol_log2_token( ' � ' )

	$mol_log2_legend.info( $mol_atom2_token_revalidation , '$mol_atom2 starts masters cheking for changes' )
	$mol_log2_legend.info( $mol_atom2_token_stumbled , '$mol_atom2 is obsoleted while masters checking' )
	$mol_log2_legend.info( $mol_atom2_token_revalidated , '$mol_atom2 is actual becasue there is no changed masters' )
	$mol_log2_legend.info( $mol_atom2_token_leaded , '$mol_atom2 leads some slave' )
	$mol_log2_legend.info( $mol_atom2_token_disleaded , '$mol_atom2 disleads some slave' )
	$mol_log2_legend.info( $mol_atom2_token_obsoleted , '$mol_atom2 is obsoleted because some master is changed' )
	$mol_log2_legend.info( $mol_atom2_token_doubted , '$mol_atom2 is doubted because some master is doubted or obsoleted' )

}
