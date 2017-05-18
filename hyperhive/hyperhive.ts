declare var hhfw : any
declare var sqlitePlugin : any

namespace $ {

	export class $mol_hyperhive extends $mol_object {

		host() { return '' }
		version() { return '' }
		environment() { return '' }
		project() { return '' }
		application() { return '' }
		
		@ $mol_mem()
		login( next? : string ) {
			return next || ''
		}

		@ $mol_mem()
		password( next? : string ) {
			return next || ''
		}

		@ $mol_mem()
		device() {
			return Date.now().toString( 16 )
		}

		@ $mol_mem_key()
		static item( config : {
			host : string
			version : string
			environment : string
			project : string
			application : string
		} ) {
			const next = new $mol_hyperhive()
			next.host = ()=> config.host
			next.version = ()=> config.version
			next.environment = ()=> config.environment
			next.project = ()=> config.project
			next.application = ()=> config.application
			return next
		}
		
		@ $mol_mem()
		initialized() {
			if( typeof hhfw === 'undefined' ) return true
			
			hhfw.Init( `http://${ this.host() }` , this.version() , this.environment() , this.project() , this.application() , '123' )
			
			hhfw.SetSslChecks( false )
			
			return true
		}
		
		@ $mol_mem()
		authentificated( next? : boolean , force? : $mol_atom_force ) : boolean {

			if( !this.login() || !this.password() ) return false

			if( typeof hhfw === 'undefined' ) {
				
				const uri = `http://${ this.host() }/api/${ this.version() }/auth/?env=${ this.environment() }&proj=${ this.project() }&app=${ this.application() }`
				const res = $mol_http_resource_json.item<any>( uri )
				
				res.credentials = ()=> ({
					login : this.login() ,
					password : this.password() ,
				})
				
				res.headers = ()=> ({
					'x-device-id' : this.device()
				})
				
				return res.json().valueOf() && true
			}

			this.initialized().valueOf()

			hhfw.Auth(
				this.login() ,
				this.password() ,
				( message : any )=> this.authentificated( true , $mol_atom_force ) ,
				( message : any )=> this.authentificated( new Error( message ) as any , $mol_atom_force ) ,
			)
			
			throw new $mol_atom_wait( 'Authentification...' )
		}
		
		@ $mol_mem()
		resources( next? : any , force? : $mol_atom_force ) : boolean {
			this.authentificated().valueOf()

			hhfw.GetResource(
				( message : any )=> this.resources( JSON.parse( message ).data , $mol_atom_force ) ,
				( message : any )=> this.resources( new Error( `${ message }` ) as any , $mol_atom_force ) ,
			)
			
			throw new $mol_atom_wait( 'Loading resource list...' )
		}
		
		@ $mol_mem_key()
		data< Value >( table : string , next? : any , force? : $mol_atom_force ) : Value {
			
			if( typeof hhfw === 'undefined' ) {
				this.authentificated().valueOf()

				const uri_descr = `http://${ this.host() }/api/${ this.version() }/resources_description/`
				const descr = $mol_http_resource_json.item< any >( uri_descr ).json()
				
				const uri_data = `http://${ this.host() }/api/${ this.version() }/table/GET_${ table }/`
				const res = $mol_http_resource_json.item< any >( uri_data )
				res.method_get = $mol_const( 'Post' )
				res.credentials = $mol_const({})
				const table_data = res.json()

				const data = table_data.map( ( values : any[] )=> {
					const record = {} as any
					values.forEach( ( val , index )=> {
						const field = Object.keys( descr[ `GET_${ table }`].output[ index ] )[0]
						record[ field ] = val
					} )
					return record
				} )

				return data as Value
			}

			this.resources().valueOf()
			
			const handleError = ( message : string ) => {
				const error = new Error( message )
				this.data( table , error , $mol_atom_force )
			}
			
			if( next === void 0 ) {
				hhfw.GetDeltaStream(
					`GET_${ table }` ,
					( result : any ) => {
						setTimeout( ()=> {
							hhfw.Query(
								`select * from GET_${ table }` ,
								( resp : string )=> this.data( table , JSON.parse( resp ).data || [] , $mol_atom_force ) ,
								handleError ,
							)
						})
					} ,
					handleError ,
				)
			} else {
				hhfw.Post(
					`UPSERT_${ table }` ,
					table ,
					JSON.stringify( next ) ,
					( resp : any )=> this.data( table , void 0 , $mol_atom_force ) ,
					handleError
				)
			}
			
			throw new $mol_atom_wait( `Loading ${ table }...` )
		
		}
		
	}
}
