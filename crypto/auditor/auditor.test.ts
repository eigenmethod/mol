namespace $ {
	$mol_test({
		
		async 'sizes'() {
			
			const pair = await $$.$mol_crypto_auditor_pair()
			
			const key_private = await pair.private.serial()
			$mol_assert_ok( key_private!.byteLength > 190 )
			$mol_assert_ok( key_private!.byteLength < 200 )
			
			const key_public = await pair.public.serial()
			$mol_assert_equal( key_public!.byteLength, 62 )
			
			const data = new Uint8Array([1,2,3])
			const sign = await pair.private.sign( data )
			$mol_assert_equal( sign.byteLength, 32 )
			
		},
		
		async 'verify self signed with auto generated key'() {
			
			const auditor = await $$.$mol_crypto_auditor_pair()
			const data = new Uint8Array([1,2,3])
			const sign = await auditor.private.sign( data )
			
			$mol_assert_ok( await auditor.public.verify( data, sign ) )
			
		},
		
		async 'verify signed with exported auto generated key'() {
			
			const pair = await $$.$mol_crypto_auditor_pair()
			const data = new Uint8Array([1,2,3])
			
			const Alice = await $mol_crypto_auditor_private.from( await pair.private.serial() )
			const sign = await Alice.sign( data )
			
			const Bob = await $mol_crypto_auditor_public.from( await pair.public.serial() )
			$mol_assert_ok( await Bob.verify( data, sign ) )
			
		},
		
	})
}
