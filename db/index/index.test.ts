namespace $ {
	$mol_test({
		
		async 'unique index'() {
			
			const db = await $$.$mol_db<{
				users: {
					Key: string
					Doc: { name: string }
					Indexes: {
						names: [ string ]
					}
				}
			}>( '$mol_db_test',
				mig => mig.store_make( 'users' ),
				mig => mig.stores.users.index_make( 'names', [ 'name' ], !!'unique' ),
			)
			const trans = db.change( 'users' )
			
			try {
			
				const { users } = trans.stores
				await users.put( { name: 'Jin' }, 'jin' )
				await users.put( { name: 'John' }, 'john' )

				const { names } = users.indexes
				$mol_assert_like( await names.get( [ 'Jin' ], 10 ), [{ name: 'Jin' }] )
				$mol_assert_like( await names.get( [ 'John' ], 10 ), [{ name: 'John' }] )
				$mol_assert_like( await names.count(), 2 )
				
				try {
					await users.put( { name: 'Jin' }, 'jin2' )
					$mol_fail( new Error( 'Exception expected' ) )
				} catch( error ) {
					$mol_assert_equal( error.message, `Unable to add key to index 'names': at least one key does not satisfy the uniqueness requirements.` )
				}
				
			} finally {
				
				trans.abort()
				db.kill()
				
			}
			
		},
		
		async 'multi path index'() {
			
			const db = await $$.$mol_db<{
				users: {
					Key: string
					Doc: { first: string, last: string }
					Indexes: {
						names: [ string, string ]
					}
				}
			}>( '$mol_db_test',
				mig => mig.store_make( 'users' ),
				mig => mig.stores.users.index_make( 'names', [ 'first', 'last' ] ),
			)
			const trans = db.change( 'users' )
			
			try {
			
				const { users } = trans.stores
				await users.put( { first: 'Jin', last: 'Johnson' }, 'jin' )
				await users.put( { first: 'John', last: 'Jinson' }, 'john' )
				
				const { names } = users.indexes
				$mol_assert_like( await names.get( [ 'Jin', 'Johnson' ], 10 ), [{ first: 'Jin', last: 'Johnson' }] )
				$mol_assert_like( await names.get( [ 'John', 'Jinson' ], 10 ), [{ first: 'John', last: 'Jinson' }] )
				$mol_assert_like( await names.count(), 2 )
				
			} finally {
				
				trans.abort()
				db.kill()
				
			}
			
		},
		
		async 'multiple indexes'() {
			
			const db = await $$.$mol_db<{
				users: {
					Key: string
					Doc: { name: string, age: number }
					Indexes: {
						names: [ string ]
						ages: [ number ]
					}
				}
			}>( '$mol_db_test',
				mig => mig.store_make( 'users' ),
				mig => mig.stores.users.index_make( 'names', [ 'name' ], !!'unique' ),
				mig => mig.stores.users.index_make( 'ages', [ 'age' ] ),
			)
			const trans = db.change( 'users' )
			
			try {
			
				const { users } = trans.stores
				await users.put( { name: 'Jin', age: 18 }, 'jin' )
				await users.put( { name: 'John', age: 18 }, 'john' )
				
				const { names, ages } = users.indexes
				
				$mol_assert_like( await names.get( [ 'Jin' ], 10 ), [ { name: 'Jin', age: 18 } ] )
				$mol_assert_like( await names.get( [ 'John' ], 10 ), [ { name: 'John', age: 18 } ] )
				$mol_assert_like( await names.count(), 2 )
				
				$mol_assert_like( await ages.get( [ 18 ] ), [ { name: 'Jin', age: 18 } ] )
				$mol_assert_like( await ages.get( [ 18 ], 10 ), [ { name: 'Jin', age: 18 }, { name: 'John', age: 18 } ] )
				$mol_assert_like( await ages.count(), 2 )
				
			} finally {
				
				trans.abort()
				db.kill()
				
			}
			
		},
		
	})
}
