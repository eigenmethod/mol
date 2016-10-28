module $.$mol {
	export class $mol_app_supplies_lister extends $.$mol_app_supplies_lister {
		
		requests() {
			return [] as $mol_app_supplies_domain_supply[]
		}
		
		@ $mol_mem()
		supplyRows() {
			return this.supplies().map( ( supply , index ) => this.supplyRow( index ) )
		}
		
		@ $mol_mem_key()
		supplyRow( index : number ) {
			return new $mol_app_supplies_carder().setup( obj => {
				obj.supply = ()=> this.supplies()[ index ]
				obj.arg = ()=> ({
					supply : ()=> this.supplies()[ index ].id() ,
					side : ()=> <string> null
				})
			} )
		}
		
	}
}
